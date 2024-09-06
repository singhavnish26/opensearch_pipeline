import datetime
from datetime import datetime, timedelta
import calendar
import requests
import json
import urllib3
import configparser
import csv  

config = configparser.ConfigParser()
config.read('credentials.ini')

# Silence API warning errors
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# API input
while True:
    API_input = input("Enter API type (MDM or HES): ")
    
    if API_input == "MDM":
        add = config['API']['MDM_add']
        user = config['API']['MDM_username']
        password = config['API']['MDM_password']
        break
    elif API_input == "HES":
        add = config['API']['HES_add']
        user = config['API']['HES_username']
        password = config['API']['HES_password']
        break
    else:
        print("Invalid Input, please try again.")

# Profile input
profile_matrix = {
    "Billing": "1-0:98.1.0*255",
    "Daily": "1-0:99.2.0*255",
    "BlockLoad": "1-0:99.1.0*255",
    "Instantaneous": "1-0:94.91.0*255"
}

while True:
    print("\nFor Billing Profile Write 'Billing'")
    print("For Daily Profile Write 'Daily'")
    print("For BlockLoad Profile Write 'BlockLoad'")
    print("For Instantaneous Profile Write 'Instantaneous'")
    
    prof = input("Enter the profile you want to get data for: ")
    
    profile = profile_matrix.get(prof)
    
    if profile:
        print(f"Profile selected: {prof}, Code: {profile}")
        break
    else:
        print("Invalid profile, please try again.\n")

def generate_time_data(month: str = None, year: int = None, start_hour: int = 18, start_minute: int = 29):
    now = datetime.now()
    year = year or now.year
    month = month.capitalize() if month else now.strftime('%B')
    
    cur_start_date = datetime(year, list(calendar.month_name).index(month), 
                              calendar.monthrange(year, list(calendar.month_name).index(month))[1],
                              start_hour, start_minute)
    
    return {
        "start_time_cur": cur_start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to_time_cur": (cur_start_date + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

month_input = input("Enter the month (e.g., January) or press Enter to use the current month: ")
year_input = input("Enter the year (e.g., 2024) or press Enter to use the current year: ")
month = month_input if month_input else None
year = int(year_input) if year_input else None
time_data = generate_time_data(month, year)
start_time_cur, to_time_cur = time_data["start_time_cur"], time_data["to_time_cur"]

# Device URL
url1 = f"{add}/api/1/devices/"

# Get device list
def get_devices():
    try:
        response = requests.get(url1, auth=(user, password), verify=False)
        response.raise_for_status()
        devices = response.json()
        
        # Clean up device dictionary by removing unnecessary keys
        del_keys = ['communicationId', 'typeId', 'typeName', 'templateId', 'templateName', 
                    'managementState', 'description', 'manufacturer', 'model', 
                    'parentId', 'location', 'storeData', 'groupId']
        for device in devices:
            for key in del_keys:
                device.pop(key, None)
        return devices
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return []

# Get profile data
def get_profile_data(profile, devices, from_time, to_time):
    m_values_kwh, m_values_kvah = [], []
    
    for device in devices:
        device_id, group_name = device.get('id', 'N/A'), device.get('groupName', 'N/A')
        url2 = f"{add}/api/1/devices/{device_id}/profiles/{profile}/entries?from={from_time}&to={to_time}"
        
        try:
            response = requests.get(url2, auth=(user, password), verify=False)
            response.raise_for_status()
            entries = response.json().get("entries", [])
            
            if entries:
                metered_values = entries[0].get("meteredValues", [])
                for item in metered_values:
                    item.update({"device": device_id, "profile": profile, "groupName": group_name})
                    if item["registerId"] == "1-0:1.8.0*255":
                        m_values_kwh.append(item)
                    elif item["registerId"] == "1-0:9.8.0*255":
                        m_values_kvah.append(item)
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            continue
    
    return m_values_kwh, m_values_kvah

# Check data availability
def check_data_availability(devices, current_kwh_values):
    available_devices = [item['device'] for item in current_kwh_values]
    result = []
    
    for device in devices:
        device_id = device.get('id')
        is_data_available = device_id in available_devices
        measured_at = next((entry['measuredAt'] for entry in current_kwh_values if entry['device'] == device_id), to_time_cur)
        result.append({
            'groupName': device.get('groupName'),
            'device': device_id,
            'isDataAvailable': is_data_available,
            'measuredAt': measured_at
        })
    
    return result

# Write dictionaries to CSV
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

dev_list = get_devices()
print(f"Starting data extraction for {len(dev_list)} devices at: {datetime.now()}")
kwh_values, kvah_values = get_profile_data(profile, dev_list, start_time_cur, to_time_cur)
print(f"Extracted data for {len(kwh_values)} devices at: {datetime.now()}")
data_avail = check_data_availability(dev_list, kwh_values)

year_str = year_input if year_input else str(datetime.now().year)
month_str = month_input if month_input else datetime.now().strftime('%B')

file_name = "deviceList"
write_dicts_to_csv(dev_list, f"{file_name}_{year_str}_{month_str}.csv")
print(f"Device List written in file: {file_name}_{year_str}_{month_str}.csv")

file_name = "kWhValues"
write_dicts_to_csv(kwh_values, f"{file_name}_{year_str}_{month_str}.csv")
print(f"kWh Values written in file: {file_name}_{year_str}_{month_str}.csv")

file_name = "kVAhValues"
write_dicts_to_csv(kvah_values, f"{file_name}_{year_str}_{month_str}.csv")
print(f"kVAh Values written in file: {file_name}_{year_str}_{month_str}.csv")

file_name = "devAvail"
write_dicts_to_csv(data_avail, f"{file_name}_{year_str}_{month_str}.csv")
print(f"Data Availability Values written in file: {file_name}_{year_str}_{month_str}.csv")

print("Script Complete")