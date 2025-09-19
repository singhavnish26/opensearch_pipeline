#!/usr/bin/env python3
import time
import json
import urllib3
import requests
import logging
from datetime import datetime, timedelta
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="daily_profile_report.log",
    filemode="a"
)
logger = logging.getLogger(__name__)

MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
url1 = MDMSaddress+"/api/1/devices"
url2 = MDMSaddress+"/api/1/bulk/device-profiles/metered-data/get"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Opensearch details
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),
    use_ssl=False,
    verify_certs=False
)
writer = OSWriter(client)

logger.info("Initialized OpenSearch client.")

group_to_state = {
    "Bokaro": "Jharkhand",
    "Chandrapura": "Jharkhand",
    "Durgapur": "West Bengal",
    "Koderma": "Jharkhand",
    "Mejia": "West Bengal",
    "Panchet": "Jharkhand",
    "Raghunathpur": "West Bengal"
}

def split_group(group):
    logger.info("Splitting group: %s", group)
    lstring = group.split()
    # Finding Consumer type
    if "domestic" in group.lower():
        consumerType = "Domestic"
    elif "commercial" in group.lower():
        consumerType = "Commercial"
    else:
        consumerType = "Unknown"

    # Finding Pay Type
    if "prepaid" in group.lower():
        payType = "Prepaid"
    elif "postpaid" in group.lower():
        payType = "Postpaid"
    else:
        payType = "Unknown"

    # Finding State
    if lstring[0] == "Maithon" or lstring[0] == "Maithon/GOMDs":
        if "JH" in lstring:
            state = "Jharkhand"
        elif "WB" in lstring:
            state = "West Bengal"
        else:
            state = None
    else:
        state = group_to_state.get(lstring[0])

    items_to_remove = ["Commercial", "Domestic", "Prepaid", "Postpaid", "JH", "WB", "prepaid", "postpaid", "domestic", "commercial"]
    group_name = ' '.join([word for word in lstring if word not in items_to_remove])
    result = {"groupName": group_name, "state": state, "consumerType": consumerType, "payType": payType}
    logger.info("Split result: %s", result)
    return result

def get_devices():
    logger.info("Fetching devices from MDM API.")
    notOver = True
    while notOver:
        try:
            r = requests.get(url1, verify=False, auth=(muser, msecret))
            r.raise_for_status()
            logger.info("Successfully fetched devices from MDM API.")
            notOver = False
        except requests.exceptions.RequestException as e:
            logger.error("Error fetching devices: %s", e)
            time.sleep(5)
    devList = json.loads(r.content.decode('utf-8'))
    logger.info("Total devices fetched: %d", len(devList))
    return devList

devices = get_devices()
devices_v2 = []
for item in devices:
    result = split_group(item["groupName"])
    last_connection = item["lastConnection"] if item["lastConnection"] is not None else 0
    delta = int(time.time()) - last_connection
    devices_v2.append({
        "deviceId": item["id"],
        "group": result
    })
logger.info("Processed devices into devices_v2.")

ALLOWED_REGS = {
    "1-0:2.8.0*255",
    "1-0:10.8.0*255",
    "1-0:1.8.0*255",
    "1-0:9.8.0*255"
}
fromTime, toTime, msmtTime = [
    (datetime.utcnow() - timedelta(days=1)).strftime(f"%Y-%m-%dT{t}:00Z")
    for t in ("18:29", "18:31", "18:30")
]

logger.info("Time range for data extraction: from %s to %s.", fromTime, toTime)

batchSize = 2000
count = 1
postData = []
batchDeviceList = []
batchGroupList = []
deviceList = []
groupList = []

for item in devices_v2:
    deviceList.append(item['deviceId'])
    groupList.append(item["group"])
logger.info("Prepared device and group lists.")

data_dictionary = []

for idx, deviceId in enumerate(deviceList):
    postData.append({
        "device": deviceId,
        "profile": "1-0:99.2.0*255",
        "from": fromTime,
        "to": toTime
    })
    batchDeviceList.append(deviceId)
    batchGroupList.append(groupList[idx])

    if (len(postData) == batchSize) or (count == len(deviceList)):
        logger.info("Processing batch %d.", count)
        notOver = True
        while notOver:
            try:
                r = requests.post(url2, json=postData, verify=False, auth=(muser, msecret))
                r.raise_for_status()
                logger.info("Successfully fetched batch data.")
                notOver = False
            except requests.exceptions.RequestException as e:
                logger.error("Error fetching batch data: %s", e)
                time.sleep(5)

        returnJSON = json.loads(r.content.decode('utf-8'))
        logger.info("Processing returned JSON data.")

        j = 0
        for entry in returnJSON:
            if entry.get("success"):
                valueList = entry.get("value", [])
                for value in valueList:
                    for registerValue in value.get("meteredValues", []):
                        reg_id = registerValue.get("registerId")
                        if not reg_id or reg_id not in ALLOWED_REGS:
                            continue
                        data_dictionary.append({
                            "deviceId": batchDeviceList[j],
                            "groupName": batchGroupList[j],
                            "registerId": registerValue.get("registerId"),
                            "unit": registerValue.get("unit"),
                            "value": registerValue.get("value"),
                            "measuredAt": registerValue.get("measuredAt")
                        })
            j += 1

        postData = []
        batchDeviceList = []
        batchGroupList = []
        logger.info("Completed batch %d.", count)

    count += 1

logger.info("Processing data dictionary for OpenSearch.")
for item in data_dictionary:
    group_dict = item["groupName"]
    item.update({
        "groupName": group_dict.get("groupName"),
        "state": group_dict.get("state"),
        "consumerType": group_dict.get("consumerType"),
        "payType": group_dict.get("payType")
    })
    item.pop("groupName", None)

dev_with_data = set()
data_avail = []
for item in data_dictionary:
    dev_with_data.add(item["deviceId"])
for item in devices_v2:
    isDataAvail = item["deviceId"] in dev_with_data
    group_dict = item["group"]
    data_avail.append({
        "deviceId": item["deviceId"],
        "groupName": group_dict.get("groupName"),
        "state": group_dict.get("state"),
        "consumerType": group_dict.get("consumerType"),
        "payType": group_dict.get("payType"),
        "dataAvailable": isDataAvail,
        "measuredAt": msmtTime
    })

index1 = "dp_data-" + datetime.now().strftime("%y-%m")
index2 = "dp_avail-" + datetime.now().strftime("%y-%m")

logger.info("Pushing data to OpenSearch index: %s", index1)
writer.push(index_name=index1, docs=data_dictionary, id_field=None)
logger.info("Pushed %d documents to index %s.", len(data_dictionary), index1)

logger.info("Pushing availability data to OpenSearch index: %s", index2)
writer.push(index_name=index2, docs=data_avail, id_field=None)
logger.info("Pushed %d documents to index %s.", len(data_avail), index2)