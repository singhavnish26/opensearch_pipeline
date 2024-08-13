import datetime
import requests
import json
import urllib3
import configparser
from opensearchpy import OpenSearch, exceptions

config = configparser.ConfigParser()
config.read('credentials.ini')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Print the start time of the script
print(f"Script started at: {datetime.datetime.now()}")

MDMadd = config['API']['MDM_add']
user = config['API']['username']
password = config['API']['password']
OSS_add = config['OSS']['OSS_add']
OSS_port = int(config['OSS']['OSS_port'])
OSS_user = config['OSS']['username']
OSS_password = config['OSS']['password']

profile = "1-0:98.1.0*255"
start_time_cur = "2024-06-30T18:29:00Z"
to_time_cur = "2024-06-30T18:31:00Z"
start_time_prev = "2024-05-31T18:29:00Z"
to_time_prev = "2024-05-31T18:31:00Z"
url1 = f"{MDMadd}/api/1/devices/"
data_index = 'billing_profile_data'
avail_index = 'billing_data_avail'

client = OpenSearch(
    hosts=[{"host": OSS_add, "port": OSS_port}],
    http_auth=(OSS_user, OSS_password),
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

def get_devices():
    try:
        r = requests.get(url1, auth=(user, password), verify=False)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return []
    
    device = r.json()
    
    del_key = ['communicationId', 'typeId', 'typeName', 'templateId', 'templateName', 'managementState', 'description', 'manufacturer', 'model', 'parentId', 'location', 'storeData', 'groupId']
    for item in device:
        for k in del_key:
            item.pop(k, None)
    return device

def get_profile_data(profile, devlist, from_time, to_time):
    m_values_kwh = []
    m_values_kvah = []
    for device in devlist:
        device_id = device.get('id', 'N/A')
        groupName = device.get('groupName', 'N/A')
        url2 = f"{MDMadd}/api/1/devices/{device_id}/profiles/{profile}/entries?from={from_time}&to={to_time}"
        try:
            r2 = requests.get(url2, auth=(user, password), verify=False)
            r2.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            continue
        
        response_dict = r2.json()
        entries = response_dict.get("entries", [])
        if entries:
            first_entry = entries[0]
            metered_value = first_entry.get("meteredValues", [])

            for item in metered_value:
                if item["registerId"] == "1-0:1.8.0*255":
                    item["device"] = device_id
                    item["profile"] = profile
                    item["groupName"] = groupName
                    m_values_kwh.append(item)
                elif item["registerId"] == "1-0:9.8.0*255":
                    item["device"] = device_id
                    item["profile"] = profile
                    item["groupName"] = groupName
                    m_values_kvah.append(item)
    
    return m_values_kwh, m_values_kvah

def get_diff(current_value, previous_value):
    prev_dict = {item["device"]: item["value"] for item in previous_value}
    return [
        {
            'groupName': item['groupName'],
            'device': item['device'],
            'profile': item['profile'],
            'registerId': item['registerId'],
            'unit': item['unit'],
            'measuredAt': item['measuredAt'], 
            'currentValue' : item['value'],#asked as per Pradeep to show current month's value
            'previousValue' : prev_dict.get(item['device'], #previous Month's Value
            'value': item['value'] - prev_dict.get(item['device'], 0)
        }
        for item in current_value if item['device'] in prev_dict
    ]

def check_data_availability(dev_list, value_curr_kwh):
    devices_with_data = [item['device'] for item in value_curr_kwh]
    result_list = []
    
    for item in dev_list:
        device = item.get('id')
        is_data_available = device in devices_with_data
        measured_at = next((entry['measuredAt'] for entry in value_curr_kwh if entry['device'] == device), to_time_cur)
        result_list.append({
            'groupName': item.get('groupName'),
            'device': device,
            'isDataAvailable': is_data_available,
            'measuredAt': measured_at
        })
    
    return result_list

def test_opensearch_connection():
    try:
        response = client.info()
        print("Connection successful!")
        print("OpenSearch info:", response)
    except Exception as e:
        print(f"Connection failed: {e}")

def bulk_insert_to_opensearch(index_name, *data_dicts):
    actions = []
    for data in data_dicts:
        for item in data:
            action = {"index": {"_index": index_name}}
            actions.append(action)
            actions.append(item)

    if not actions:
        print("No data to insert")
        return

    try:
        response = client.bulk(body=actions)
        print("Bulk insert operation completed.")
    except exceptions.OpenSearchException as e:
        print("Error during bulk insert:", e)

dev_list = get_devices()
print(f"Starting Extracting Data for Current Month {len(dev_list)} Devices at: {datetime.datetime.now()}")
value_curr_kwh, value_curr_kvah = get_profile_data(profile, dev_list, start_time_cur, to_time_cur)
print(f"Extracted Data for Current Month {len(value_curr_kwh)} Devices at: {datetime.datetime.now()}")

print(f"Starting Extracting Data for Previous Month {len(dev_list)} Devices at: {datetime.datetime.now()}")
value_prev_kwh, value_prev_kvah = get_profile_data(profile, dev_list, start_time_prev, to_time_prev)
print(f"Extracted Data for Previous Month {len(value_prev_kwh)} Devices at: {datetime.datetime.now()}")

diff_kwh = get_diff(value_curr_kwh, value_prev_kwh)
diff_kvah = get_diff(value_curr_kvah, value_prev_kvah)
data_avail=check_data_availability(dev_list,value_curr_kwh)
test_opensearch_connection()
bulk_insert_to_opensearch(data_index, diff_kvah, diff_kwh)
bulk_insert_to_opensearch(avail_index, data_avail)