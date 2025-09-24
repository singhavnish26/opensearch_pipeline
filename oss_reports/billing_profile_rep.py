#!/usr/bin/env python3
import time
import json
import urllib3
import requests
import logging
import calendar
from datetime import datetime, timedelta, date, timezone
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter
import warnings

# ----------------------------------------------------------------------
# Suppress OpenSearch warnings
# ----------------------------------------------------------------------
warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")

# ----------------------------------------------------------------------
# Logging Configuration
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="daily_profile_report.log",
    filemode="a"
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Static Data for MDM API
# ----------------------------------------------------------------------
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"

url = f"{MDMSaddress}/api/1/devices"
url1 = MDMSaddress + "/api/1/devices"
url2 = MDMSaddress + "/api/1/bulk/device-profiles/metered-data/get"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------------------------------------------------
# OpenSearch Connection
# ----------------------------------------------------------------------
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),
    use_ssl=False,
    verify_certs=False
)
writer = OSWriter(client)

logger.info("Initialized OpenSearch client.")

# ----------------------------------------------------------------------
# Groups and Allowed Registers
# ----------------------------------------------------------------------
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
    "1-0:2.8.0*255",   # kWh export
    "1-0:10.8.0*255",  # kVAh export
    "1-0:1.8.0*255",   # kWh import
    "1-0:9.8.0*255"    # kVAh import
}

# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------
def split_group(group):
    """
    Split the group string into groupName, state, consumerType, and payType.
    """
    lstring = group.split()

    # Consumer type
    if "domestic" in group.lower():
        consumerType = "Domestic"
    elif "commercial" in group.lower():
        consumerType = "Commercial"
    else:
        consumerType = "Unknown"

    # Pay type
    if "prepaid" in group.lower():
        payType = "Prepaid"
    elif "postpaid" in group.lower():
        payType = "Postpaid"
    else:
        payType = "Unknown"

    # State
    if lstring[0] in ("Maithon", "Maithon/GOMDs"):
        if "JH" in lstring:
            state = "Jharkhand"
        elif "WB" in lstring:
            state = "West Bengal"
        else:
            state = None
    else:
        state = group_to_state.get(lstring[0])

    # Cleaned group name
    items_to_remove = [
        "Commercial", "Domestic", "Prepaid", "Postpaid",
        "JH", "WB", "prepaid", "postpaid", "domestic", "commercial"
    ]
    group_name = " ".join([word for word in lstring if word not in items_to_remove])

    return {
        "groupName": group_name,
        "state": state,
        "consumerType": consumerType,
        "payType": payType
    }


def generate_time_data_auto(ref=None):
    """
    Generate time data for current and previous month.
    """
    ref = ref or date.today()

    def shift(y, m, d):
        t = y * 12 + m - 1 + d
        return t // 12, t % 12 + 1

    def last_day(y, m):
        return date(y, m, calendar.monthrange(y, m)[1])

    def fmt(d, h, m):
        return datetime(d.year, d.month, d.day, h, m, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    y1, m1 = shift(ref.year, ref.month, -1)
    y2, m2 = shift(ref.year, ref.month, -2)

    d1, d2 = last_day(y1, m1), last_day(y2, m2)

    return {
        "start_time_cur":  fmt(d1, 18, 29),
        "to_time_cur":     fmt(d1, 18, 31),
        "start_time_prev": fmt(d2, 18, 29),
        "to_time_prev":    fmt(d2, 18, 31),
        "msmtTime":        fmt(d1, 18, 30)
    }


def get_devices():
    """
    Get devices list from NB API.
    """
    notOver = True
    while notOver:
        try:
            r = requests.get(url1, verify=False, auth=(muser, msecret))
            r.raise_for_status()
            notOver = False
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException):
            continue

    devList = json.loads(r.content.decode("utf-8"))
    return devList


def get_billing_profile(fromTime, toTime, profile):
    """
    Get billing profile data from NB API.
    """
    batchSize = 2000
    count = 1
    postData = []
    batchDeviceList = []
    batchGroupList = []
    deviceList = []
    groupList = []

    for item in devices_v2:
        deviceList.append(item["deviceId"])
        groupList.append(item["groupName"])

    data_dictionary = []  # Accumulator

    # Loop over devices in batches
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
                except (requests.exceptions.HTTPError,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.RequestException):
                    continue

            returnJSON = json.loads(r.content.decode("utf-8"))

            # Map returned entries back to the batch device/group lists
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

            # Reset for next batch
            postData = []
            batchDeviceList = []
            batchGroupList = []

        count += 1

    # Availability check
    data_avail = []
    dev_with_data = {item["deviceId"] for item in data_dictionary}

    for item in devices_v2:
        isDataAvail = item["deviceId"] in dev_with_data
        data_avail.append({
            "deviceId": item["deviceId"],
            "groupName": item["groupName"],
            "isDataAvail": isDataAvail,
            "measuredAt": time_data["msmtTime"]
        })

    return data_dictionary, data_avail


def get_diff(current_value, previous_value):
    """
    Get difference between current and previous month values.
    """
    prev_dict = {item["deviceId"]: item["value"] for item in previous_value}
    return [
        {
            "groupName":  item["groupName"],
            "deviceId":   item["deviceId"],
            "registerId": item["registerId"],
            "unit":       item["unit"],
            "measuredAt": item["measuredAt"],
            "value":      item["value"] - prev_dict.get(item["deviceId"], 0)
        }
        for item in current_value if item["deviceId"] in prev_dict
    ]

# ----------------------------------------------------------------------
# Main Execution
# ----------------------------------------------------------------------
devices = get_devices()
devices_v2 = []

for item in devices:
    result = split_group(item["groupName"])
    last_connection = item["lastConnection"] if item["lastConnection"] is not None else 0
    delta = int(time.time()) - last_connection

    devices_v2.append({
        "deviceId": item["id"],
        "groupName": result
    })

time_data = generate_time_data_auto()

bp_data_curr, data_avail_curr = get_billing_profile(
    "2025-08-31T18:29:00Z", "2025-08-31T18:31:00Z", "1-0:98.1.0*255"
)
bp_data_prev, data_avail_prev = get_billing_profile(
    "2025-07-31T18:29:00Z", "2025-07-31T18:31:00Z", "1-0:98.1.0*255"
)

# Attach group details to results
for item in bp_data_curr + bp_data_prev + data_avail_curr:
    group_dict = item["groupName"]
    item.update({
        "groupName":    group_dict.get("groupName"),
        "state":        group_dict.get("state"),
        "consumerType": group_dict.get("consumerType"),
        "payType":      group_dict.get("payType")
    })

# Split registers
curr_export_kwh  = [i for i in bp_data_curr if i["registerId"] == "1-0:2.8.0*255"]
curr_export_kvah = [i for i in bp_data_curr if i["registerId"] == "1-0:10.8.0*255"]
curr_import_kwh  = [i for i in bp_data_curr if i["registerId"] == "1-0:1.8.0*255"]
curr_import_kvah = [i for i in bp_data_curr if i["registerId"] == "1-0:9.8.0*255"]

prev_export_kwh  = [i for i in bp_data_prev if i["registerId"] == "1-0:2.8.0*255"]
prev_export_kvah = [i for i in bp_data_prev if i["registerId"] == "1-0:10.8.0*255"]
prev_import_kwh  = [i for i in bp_data_prev if i["registerId"] == "1-0:1.8.0*255"]
prev_import_kvah = [i for i in bp_data_prev if i["registerId"] == "1-0:9.8.0*255"]

# Differences
diff_export_kwh  = get_diff(curr_export_kwh, prev_export_kwh)
diff_export_kvah = get_diff(curr_export_kvah, prev_export_kvah)
diff_import_kwh  = get_diff(curr_import_kwh, prev_import_kwh)
diff_import_kvah = get_diff(curr_import_kvah, prev_import_kvah)

# Index names
index1 = "bp_data-" + datetime.now().strftime("%y-%m")
index2 = "bp_avail-" + datetime.now().strftime("%y-%m")

# Push data
logger.info("Pushing data to OpenSearch index: %s", index1)
writer.push(index_name=index1, docs=diff_export_kwh,  id_field=None)
writer.push(index_name=index1, docs=diff_export_kvah, id_field=None)
writer.push(index_name=index1, docs=diff_import_kwh,  id_field=None)
writer.push(index_name=index1, docs=diff_import_kvah, id_field=None)

total_fields = len(diff_export_kwh) + len(diff_export_kvah) + len(diff_import_kwh) + len(diff_import_kvah)
logger.info("Pushed %d documents to index %s.", total_fields, index1)

logger.info("Pushing availability data to OpenSearch index: %s", index2)
writer.push(index_name=index2, docs=data_avail_curr, id_field=None)
logger.info("Pushed %d documents to index %s.", len(data_avail_curr), index2)
