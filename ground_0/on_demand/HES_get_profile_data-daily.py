import datetime
from datetime import datetime, timedelta
import calendar
import requests
import json
import urllib3
import configparser
config = configparser.ConfigParser()
config.read('credentials.ini')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
MDMadd = config['API']['MDM_add']
MDM_user = config['API']['MDM_username']
MDM_password = config['API']['MDM_password']
HESadd = config['API']['HES_add']
HES_user = config['API']['HES_username']
HES_password = config['API']['HES_password']
url1 = f"{MDMadd}/api/1/devices/"
profile="1-0:98.1.0*255" 

def get_devices():
    try:
        r = requests.get(url1, auth=(MDM_user, MDM_password), verify=False)
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
        url2 = f"{HESadd}/api/1/devices/{device_id}/profiles/{profile}/entries?from={from_time}&to={to_time}"
        try:
            r2 = requests.get(url2, auth=(HES_user, HES_password), verify=False)
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

def write_dicts_to_csv(dict_list, file_name):
    if not dict_list:
        print("The list of dictionaries is empty.")
        return
    headers = dict_list[0].keys()
    try:
        with open(file_name, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(dict_list)
        print(f"Data successfully written to {file_name}")
    except IOError:
        print("I/O error occurred while writing to the file.")
        
def generate_time_data(day: int = None, month: str = None, year: int = None, start_hour: int = 18, start_minute: int = 29):
    now = datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.strftime('%B')
    month = month.capitalize()
    if day is None:
        day = now.day
        month = now.strftime('%B')
        year = now.year  
    cur_start_date = datetime(year, list(calendar.month_name).index(month), day, start_hour, start_minute)
    start_time_cur = cur_start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_cur = (cur_start_date + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_data = {
        "start_time_cur": start_time_cur,
        "to_time_cur": end_time_cur
    }
    return time_data
        
        
#profile=input("Enter the profile value: ")
month_input = input("Enter the month (e.g., January) or press Enter to use the current month: ")
year_input = input("Enter the year (e.g., 2024) or press Enter to use the current year: ")
day_input = input("Enter the day (e.g., 31) or press Enter to use today's date: ")
month = month_input if month_input else None
year = int(year_input) if year_input else None
day = int(day_input) if day_input else None
time_data = generate_time_data(day, month, year)
start_time_cur = time_data.get("start_time_cur")
to_time_cur = time_data.get("to_time_cur")
end_date_part = start_time_cur.split("T")[0]

dev_list = get_devices()
print(f"Starting Extracting Data for Current Month {len(dev_list)} Devices at: {datetime.now()}")
value_curr_kwh, value_curr_kvah = get_profile_data(profile, dev_list, start_time_cur, to_time_cur)
data_avail=check_data_availability(dev_list,value_curr_kwh)
print(f"Recieved KVAH & KWH Data for Current Month for {len(value_curr_kwh)} Devices at: {datetime.datetime.now()}")
print("Data Extraction Complete")
write_dicts_to_csv(dev_list, f"MDM_Device_List_{end_date_part}.csv")
write_dicts_to_csv(value_curr_kwh, f"HES_value_kwh_{end_date_part}.csv")
write_dicts_to_csv(value_curr_kvah, f"HES_value_kvah_{end_date_part}.csv")
write_dicts_to_csv(data_avail, f"HES_data_avail_{end_date_part}.csv")
print("Data Written to CSV File")