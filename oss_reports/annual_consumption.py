import time
import json
import urllib3
import requests
from datetime import datetime, timezone, timedelta, date
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter  
import calendar
import logging
from collections import defaultdict
import warnings


warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")
#Configuration
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
url1 = MDMSaddress+"/api/1/devices"
url2 = MDMSaddress+"/api/1/bulk/device-profiles/metered-data/get"


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
year = datetime.utcnow().year
fromTime = f"{year}-03-31T18:29:00Z"
toTime = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT18:31:00Z")
profile = "1-0:99.2.0*255"  # daily_profile
ALLOWED_REGS = {"1-0:1.8.0*255", "1-0:9.8.0*255"}  # kwh_register, kvah_register

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

# Configure logging to log to a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="annual_consumption.log",  # Log file path
    filemode="a"  # Append to the log file
)

def get_devices():
    logging.info("Fetching devices from MDMS...")
    notOver = True
    while notOver:
        try:
            r = requests.get(url1, verify=False, auth=(muser, msecret))
            r.raise_for_status()
            notOver = False
        except requests.exceptions.HTTPError as errh:
            logging.error("HTTP Error while fetching devices: %s", errh)
            continue
        except requests.exceptions.ConnectionError as errc:
            logging.error("Connection Error while fetching devices: %s", errc)
            continue
        except requests.exceptions.Timeout as errt:
            logging.error("Timeout Error while fetching devices: %s", errt)
            continue
        except requests.exceptions.RequestException as err:
            logging.error("Other Error while fetching devices: %s", err)
            continue
    devList = json.loads(r.content.decode('utf-8'))
    logging.info("Fetched %d devices from MDMS.", len(devList))
    return devList

deviceMasterList = get_devices()
logging.info("Total devices fetched: %d", len(deviceMasterList))

devicesFiltered = []
for item in deviceMasterList:
    if item.get("inventoryState") == "installed":
        groupName = " ".join(
            w for w in item.get("groupName", "").split()
            if w.lower() not in {"prepaid", "postpaid"}
        )
        devicesFiltered.append({"deviceId": item['id'], "groupName": groupName})
logging.info("Total installed devices: %d", len(devicesFiltered))
devicesFiltered = devicesFiltered[:100]
def get_daily_profile(fromTime, toTime, profile):
    logging.info("Fetching daily profile data from %s to %s for profile %s...", fromTime, toTime, profile)
    batchSize = 500
    count = 1
    postData = []
    batchDeviceList = []
    batchGroupList = []
    deviceList = []
    groupList = []
    for item in devicesFiltered:
        deviceList.append(item['deviceId'])
        groupList.append(item["groupName"])
    data_dictionary = []

    for idx, deviceId in enumerate(deviceList):
        postData.append({"device": deviceId, "profile": profile, "from": fromTime, "to": toTime})
        batchDeviceList.append(deviceId)
        batchGroupList.append(groupList[idx])

        if (len(postData) == batchSize) or (count == len(devicesFiltered)):
            notOver = True
            while notOver:
                try:
                    r = requests.post(url2, json=postData, verify=False, auth=(muser, msecret))
                    r.raise_for_status()
                    notOver = False
                except requests.exceptions.HTTPError as errh:
                    logging.error("HTTP Error during daily profile fetch: %s", errh)
                    continue
                except requests.exceptions.ConnectionError as errc:
                    logging.error("Connection Error during daily profile fetch: %s", errc)
                    continue
                except requests.exceptions.Timeout as errt:
                    logging.error("Timeout Error during daily profile fetch: %s", errt)
                    continue
                except requests.exceptions.RequestException as err:
                    logging.error("Other Error during daily profile fetch: %s", err)
                    continue

            returnJSON = json.loads(r.content.decode('utf-8'))

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

            postData = []
            batchDeviceList = []
            batchGroupList = []
            logging.info("Completed batch %d", count)

        count += 1
    logging.info("Daily profile data fetch completed.")
    return data_dictionary

def get_consumption_per_device(devices, data):
    logging.info("Calculating consumption per device...")
    grouped = defaultdict(list)
    for entry in data:
        grouped[entry['deviceId']].append(entry)

    result = []
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for dev in devices:
        dev_id = dev['deviceId']
        if dev_id in grouped:
            entries = grouped[dev_id]
            first, last = entries[0], entries[-1]
            diff = last['value'] - first['value']

            result.append({
                "deviceId": dev_id,
                "groupName": dev["groupName"],
                "registerId": first["registerId"],
                "unit": first["unit"],
                "value": diff,
                "measuredAt": now
            })
    logging.info("Consumption calculation completed.")
    return result


index = "annual_consumption-"+ datetime.now().strftime("%y-%m")
logging.info("Starting data processing...")
dp_data = get_daily_profile(fromTime, toTime, profile)
dp_data_kwh = [d for d in dp_data if d['registerId'] == "1-0:1.8.0*255"]
dp_data_kvah = [d for d in dp_data if d['registerId'] == "1-0:9.8.0*255"]
result_kwh = get_consumption_per_device(devicesFiltered, dp_data_kwh)
result_kvah = get_consumption_per_device(devicesFiltered, dp_data_kvah)
merged_result = result_kwh + result_kvah
merged_result.sort(key=lambda x: (x['deviceId'], x['registerId']))
logging.info("Pushing data to OpenSearch...")
writer.push(index_name=index, docs=merged_result)
#import pandas as pd
#df = pd.DataFrame(merged_result)
#df.to_csv('annual_consumption.csv', index=False)
logging.info("Data processing of %d records and push to OpenSearch completed.", len(merged_result))



