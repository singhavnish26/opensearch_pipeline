import time
import json
import urllib3
import requests
from datetime import datetime, timezone, timedelta, date
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter  
import calendar

#Config for MDM, OpenSearch and other static data
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
url1 = MDMSaddress+"/api/1/devices"
url2 = MDMSaddress+"/api/1/bulk/device-profiles/metered-data/get"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#Generate Timestamp for current and previous month
def generate_time_data_auto(ref=None):
    ref = ref or date.today()

    def shift(y, m, d):
        t = y*12 + m - 1 + d
        return t//12, t%12+1

    def last_day(y, m):
        return date(y, m, calendar.monthrange(y, m)[1])

    def fmt(d, h, m):
        return datetime(d.year, d.month, d.day, h, m, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    y1, m1 = shift(ref.year, ref.month, -1)
    y2, m2 = shift(ref.year, ref.month, -2)
    d1, d2 = last_day(y1, m1), last_day(y2, m2)

    return {
        "start_time_cur": fmt(d1, 18, 29),
        "to_time_cur":    fmt(d1, 18, 31),
        "start_time_prev":fmt(d2, 18, 29),
        "to_time_prev":   fmt(d2, 18, 31),
        "msmtTime":       fmt(d1, 18, 30)
    }
time_data = generate_time_data_auto()

print(f"Extracting data for billing profile report current month{time_data['start_time_cur']} to {time_data['to_time_cur']} and previous month {time_data['start_time_prev']} to {time_data['to_time_prev']}")
print("Fetching device list from MDM...")

#Get device list from MDM
def get_devices():
    notOver = True
    while notOver:
        try:
            r = requests.get(url1, verify=False, auth=(muser, msecret))
            r.raise_for_status()
            notOver = False
        except requests.exceptions.HTTPError as errh:
            continue
        except requests.exceptions.ConnectionError as errc:
            continue
        except requests.exceptions.Timeout as errt:
            continue
        except requests.exceptions.RequestException as err:
            continue
    devList = json.loads(r.content.decode('utf-8'))
    return devList

deviceMasterList = get_devices()
print(f"Total devices fetched: {len(deviceMasterList)}")

devicesFiltered = []
for item in deviceMasterList:
    if item.get("inventoryState") == "installed":
        groupName = " ".join(
            w for w in item.get("groupName", "").split()
            if w.lower() not in {"prepaid", "postpaid"}
        )
        devicesFiltered.append({"deviceId": item['id'], "groupName": groupName})
print(f"Total installed devices: {len(devicesFiltered)}")

#Get billing profile data from MDM
def get_billing_profile(fromTime, toTime, profile):
    batchSize = 2000
    count = 1
    postData = []
    batchDeviceList = []
    batchGroupList = []
    deviceList = []
    groupList = []
    ALLOWED_REGS = {
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
        postData.append({"device": deviceId, "profile": profile, "from": fromTime, "to": toTime})
 #       print(postData)
#        break
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
                    print("HTTP Error:", errh)
                    continue
                except requests.exceptions.ConnectionError as errc:
                    print("Connection Error:", errc)
                    continue
                except requests.exceptions.Timeout as errt:
                    print("Timeout Error:", errt)
                    continue
                except requests.exceptions.RequestException as err:
                    print("Other Error:", err)
                    continue

            returnJSON = json.loads(r.content.decode('utf-8'))

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
            print("done batch", count)

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
            "measuredAt": time_data['msmtTime']
        })
    return data_dictionary, data_avail

def get_diff(current_value, previous_value):
    prev_dict = {item["deviceId"]: item["value"] for item in previous_value}
    return [
        {
            'groupName': item['groupName'],
            'deviceId': item['deviceId'],
            'registerId': item['registerId'],
            'unit': item['unit'],
            'measuredAt': item['measuredAt'], 
            'value': item['value'] - prev_dict.get(item['deviceId'], 0)
        }
        for item in current_value if item['deviceId'] in prev_dict
    ]
    

bp_data_curr, data_avail_curr=get_billing_profile(time_data["start_time_cur"],time_data["to_time_cur"],"1-0:98.1.0*255")
bp_data_prev, data_avail_prev=get_billing_profile(time_data["start_time_prev"],time_data["to_time_prev"],"1-0:98.1.0*255")
bp_data_curr_kwh = []
bp_data_curr_kvah = []
bp_data_prev_kwh = []
bp_data_prev_kvah = []
for item in bp_data_curr:
    if item["registerId"] == "1-0:1.8.0*255":
        bp_data_curr_kwh.append(item)
    elif item["registerId"] == "1-0:9.8.0*255":
        bp_data_curr_kvah.append(item)
for item in bp_data_prev:
    if item["registerId"] == "1-0:1.8.0*255":
        bp_data_prev_kwh.append(item)
    elif item["registerId"] == "1-0:9.8.0*255":
        bp_data_prev_kvah.append(item)

diff_kwh = get_diff(bp_data_curr_kwh, bp_data_prev_kwh)
diff_kvah = get_diff(bp_data_curr_kvah, bp_data_prev_kvah)


#print(len(diff_kwh), len(diff_kvah))
#print(len(data_avail_curr), len(data_avail_prev))


#Push data to OpenSearch
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),  # or pull from env/secrets
    use_ssl=False,                 # True if your node is HTTPS
    verify_certs=False             # True + ca_certs=... if you have a real CA
)
writer = OSWriter(client)
index_name1 = "billing_profile_data-" + datetime.now().strftime("%y-%m")
index_name2 = "billing_data_avail-" + datetime.now().strftime("%y-%m")

writer.push(index_name=index_name1, docs=diff_kwh, id_field="deviceId")
writer.push(index_name=index_name1, docs=diff_kvah, id_field="deviceId")
writer.push(index_name=index_name2, docs=data_avail_curr, id_field="deviceId")
