import time
import json
import urllib3
import requests
import logging
import warnings  # Import warnings module
from datetime import datetime, timezone, timedelta
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter  
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to INFO to log only INFO and ERROR messages
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="daily_profile.log",  # Log file path
    filemode="a"  # Append to the log file
)
logger = logging.getLogger(__name__)

# Configuration
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
url1 = MDMSaddress+"/api/1/devices"
url2 = MDMSaddress+"/api/1/bulk/device-profiles/metered-data/get"
threshold = 172800  # 48 hours
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Opensearch details
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),  # or pull from env/secrets
    use_ssl=False,                 # True if your node is HTTPS
    verify_certs=False             # True + ca_certs=... if you have a real CA
)
writer = OSWriter(client)

# Suppress the specific UserWarning about insecure SSL connections
warnings.filterwarnings(
    "ignore",
    message="Connecting to https://localhost:9200 using SSL with verify_certs=False is insecure"
)

def get_devices():
    notOver = True
    while notOver:
        try:
            logger.debug("Sending request to MDM API: %s", url1)
            r = requests.get(url1, verify=False, auth=(muser, msecret))
            r.raise_for_status()
            logger.info("Successfully fetched device data from MDM API")
            notOver = False
        except requests.exceptions.HTTPError as errh:
            logger.error("HTTP Error: %s", errh)
            continue
        except requests.exceptions.ConnectionError as errc:
            logger.error("Connection Error: %s", errc)
            continue
        except requests.exceptions.Timeout as errt:
            logger.error("Timeout Error: %s", errt)
            continue
        except requests.exceptions.RequestException as err:
            logger.error("Other Error: %s", err)
            continue
    devList = json.loads(r.content.decode('utf-8'))
#    logger.debug("Received device list: %s", devList)
    return devList

deviceMasterList = get_devices()


devicesFiltered = []
for item in deviceMasterList:
    if item.get("inventoryState") == "installed":
        groupName = " ".join(
            w for w in item.get("groupName", "").split()
            if w.lower() not in {"prepaid", "postpaid"}
        )
        devicesFiltered.append({"deviceId": item['id'], "groupName": groupName})

fromTime, toTime, msmtTime = [
    (datetime.utcnow() - timedelta(days=1)).strftime(f"%Y-%m-%dT{t}:00Z")
    for t in ("18:29", "18:31", "18:30")
]

batchSize = 2000
count = 1
postData = []
batchDeviceList = []
batchGroupList = []
deviceList = []
groupList = []
ALLOWED_REGS = {
    "1-0:2.8.0*255",
    "1-0:10.8.0*255",
    "1-0:1.8.0*255",
    "1-0:9.8.0*255"
}

for item in devicesFiltered:
    deviceList.append(item['deviceId'])
    groupList.append(item["groupName"])
# Accumulator for all returned rows across batches
data_dictionary = []

# url2 already set above
for idx, deviceId in enumerate(deviceList):
    postData.append({"device": deviceId, "profile": "1-0:99.2.0*255", "from": fromTime, "to": toTime})
    batchDeviceList.append(deviceId)
    batchGroupList.append(groupList[idx])

    if (len(postData) == batchSize) or (count == len(devicesFiltered)):
        notOver = True
        while notOver:
            try:
                logger.debug("Sending batch request to MDM API: %s", url2)
                r = requests.post(url2, json=postData, verify=False, auth=(muser, msecret))
                r.raise_for_status()
                logger.info("Successfully fetched batch data from MDM API")
                notOver = False
            except requests.exceptions.HTTPError as errh:
                logger.error("HTTP Error: %s", errh)
                continue
            except requests.exceptions.ConnectionError as errc:
                logger.error("Connection Error: %s", errc)
                continue
            except requests.exceptions.Timeout as errt:
                logger.error("Timeout Error: %s", errt)
                continue
            except requests.exceptions.RequestException as err:
                logger.error("Other Error: %s", err)
                continue

        returnJSON = json.loads(r.content.decode('utf-8'))
        logger.debug("Received batch JSON response: %s", returnJSON)

        # Map returned entries back to the batch device/group lists by position
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
                            "registerId": registerValue.get("registerId"),  # <-- fixed name
                            "unit": registerValue.get("unit"),
                            "value": registerValue.get("value"),
                            "measuredAt": registerValue.get("measuredAt")
                        })
            j += 1

        # Reset for next batch
        postData = []
        batchDeviceList = []
        batchGroupList = []
        logger.info("Completed batch %d", count)

    count += 1
data_avail = []
dev_with_data = set()
for item in data_dictionary:
    dev_with_data.add(item["deviceId"])

for item in devicesFiltered:
    isDataAvail = item["deviceId"] in dev_with_data
    data_avail.append({
        "deviceId": item["deviceId"],
        "groupName": item["groupName"],
        "isDataAvail": isDataAvail,
        "measuredAt": msmtTime
    })


index1 = "dp_data-" + datetime.now().strftime("%y-%m")
index2 = "dp_avail-" + datetime.now().strftime("%y-%m")

logger.info("Pushing data to OpenSearch index: %s", index1)
writer.push(index_name=index1, docs=data_dictionary, id_field="deviceId")
logger.info("Pushed %d documents to index %s", len(data_dictionary), index1)

logger.info("Pushing availability data to OpenSearch index: %s", index2)
writer.push(index_name=index2, docs=data_avail, id_field="deviceId")
logger.info("Pushed %d documents to index %s", len(data_avail), index2)

