#!/usr/bin/env python3

import time
import json
import urllib3
import requests
import logging
from datetime import datetime
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")

# Configure logging.
logging.basicConfig(
    level=logging.INFO,  # Set to INFO to log only INFO and ERROR messages
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="device_status.log",  # Log file path
    filemode="a"
)
logger = logging.getLogger(__name__)


# Configuration
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
threshold = 172800  # 48 hours
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Mapping of group names to states
group_to_state = {
        "Bokaro": "Jharkhand",
        "Chandrapura": "Jharkhand",
        "Durgapur": "West Bengal",
        "Koderma": "Jharkhand",
        "Mejia": "West Bengal",
        "Panchet": "Jharkhand",
        "Raghunathpur": "West Bengal"
    }


#Function to split group and determine state and consumer type
def split_group(group):
    lstring = group.split()
    #state = None  # Ensure state is always defined

    #Finding Consumer type
    if "domestic" in group.lower():
        consumerType = "Domestic"
    elif "commercial" in group.lower():
        consumerType = "Commercial"
    else:
        consumerType = "Unknown"

    #Finding Pay Type
    if "prepaid" in group.lower():
        payType = "Prepaid"
    elif "postpaid" in group.lower():
        payType = "Postpaid"
    else:
        payType = "Unknown"
    #Finding State
    if lstring[0] == "Maithon" or lstring[0] == "Maithon/GOMDs":
        if "JH" in lstring:
            state = "Jharkhand"
        elif "WB" in lstring:
            state = "West Bengal"
        else:
            state = None  # State remains None if neither JH nor WB is found
    else:
        state = group_to_state.get(lstring[0])

    items_to_remove = ["Commercial", "Domestic", "Prepaid", "Postpaid", "JH", "WB", "prepaid", "postpaid", "domestic", "commercial"]
    group_name = ' '.join([word for word in lstring if word not in items_to_remove])
    result = {"Group": group_name, "State": state, "ConsumerType": consumerType, "PayType": payType}
    return result

#Fetch device status from MDM
while True:
    try:
        r = requests.get(url, verify=False, auth=(muser, msecret))
        r.raise_for_status()
        logger.info("Successfully fetched data from MDM API")
        break
    except requests.exceptions.RequestException as e:
        logger.error("API error: %s", e)
        time.sleep(5)
        continue

returnJSON = json.loads(r.content.decode("utf-8"))

dev_list = []
for item in returnJSON:
    result = split_group(item["groupName"])
    last_connection = item["lastConnection"] if item["lastConnection"] is not None else 0
    delta = int(time.time()) - last_connection
    dev_list.append({
        "deviceId": item["id"],
        "group": result["Group"],
        "state": result["State"],
        "consumerType": result["ConsumerType"],
        "payType": result["PayType"],
        "lastConnection": item["lastConnection"],
        "status": "Online" if delta < threshold else "Offline",
        "deltaTime": delta,
        "InstalledState": item["inventoryState"],
        "ingestTime": int(time.time()),
        "@timestamp": datetime.utcnow().isoformat()})
logger.info("Processed %d devices", len(dev_list))


client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),
    use_ssl=False,
    verify_certs=False
)

writer = OSWriter(client)
index_name = "device-" + datetime.now().strftime("%y-%m")

writer.push(index_name=index_name, docs=dev_list, id_field="deviceId")
logger.info("OK: pushed %d docs into %s", len(dev_list), index_name)
logger.debug("Index name: %s", index_name)

writer.push(index_name=index_name, docs=dev_list, id_field="deviceId")
logger.info("OK: pushed %d docs into %s", len(dev_list), index_name)
