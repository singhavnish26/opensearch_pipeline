
import time
import json
import urllib3
import requests
from datetime import datetime, timezone, timedelta, date
import calendar
import logging
import warnings

#warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")
#Config for MDM, OpenSearch and other static data
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
url1 = MDMSaddress+"/api/1/devices"
url2 = MDMSaddress+"/api/1/bulk/device-profiles/metered-data/get"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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


group_to_state = {
    "Bokaro": "Jharkhand",
    "Chandrapura": "Jharkhand",
    "Durgapur": "West Bengal",
    "Koderma": "Jharkhand",
    "Mejia": "West Bengal",
    "Panchet": "Jharkhand",
    "Raghunathpur": "West Bengal"
}
ALLOWED_REGS = {
    "1-0:2.8.0*255",
    "1-0:10.8.0*255",
    "1-0:1.8.0*255",
    "1-0:9.8.0*255"
}
def split_group(group):
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
    return result


devices = deviceMasterList
devices_v2 = []
for item in devices:
    result = split_group(item["groupName"])
    last_connection = item["lastConnection"] if item["lastConnection"] is not None else 0
    delta = int(time.time()) - last_connection
    devices_v2.append({
        "deviceId": item["id"],
        "groupName": result
    })


def get_billing_profile(fromTime, toTime, profile):
    batchSize = 2000
    count = 1
    postData = []
    batchDeviceList = []
    batchGroupList = []
    deviceList = []
    groupList = []


    for item in devices_v2:
        deviceList.append(item['deviceId'])
        groupList.append(item["groupName"])
    # Accumulator for all returned rows across batches
    data_dictionary = []

    # url2 already set above
    for idx, deviceId in enumerate(deviceList):
        postData.append({"device": deviceId, "profile": profile, "from": fromTime, "to": toTime})

        batchDeviceList.append(deviceId)
        batchGroupList.append(groupList[idx])

        if (len(postData) == batchSize) or (count == len(devices_v2)):
            notOver = True
            while notOver:
                try:
                    r = requests.post(url2, json=postData, verify=False, auth=(muser, msecret))
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

        count += 1
    data_avail = []
    dev_with_data = set()
    for item in data_dictionary:
        dev_with_data.add(item["deviceId"])

    for item in devices_v2:
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
diff_data = get_diff(bp_data_curr, bp_data_prev)

bp_data_curr, data_avail_curr=get_billing_profile("2025-08-31T18:29:00Z", "2025-08-31T18:31:00Z", "1-0:98.1.0*255")
bp_data_prev, data_avail_prev=get_billing_profile("2025-07-31T18:29:00Z", "2025-07-31T18:31:00Z", "1-0:98.1.0*255")
for item in diff_data:
    group_dict = item["groupName"]
    item.update({
        "groupName": group_dict.get("groupName"),
        "state": group_dict.get("state"),
        "consumerType": group_dict.get("consumerType"),
        "payType": group_dict.get("payType")
    })
for item in data_avail_curr:
    group_dict = item["groupName"]
    item.update({
        "groupName": group_dict.get("groupName"),
        "state": group_dict.get("state"),
        "consumerType": group_dict.get("consumerType"),
        "payType": group_dict.get("payType")
    })
