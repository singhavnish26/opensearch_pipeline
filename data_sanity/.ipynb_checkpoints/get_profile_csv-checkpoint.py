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
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MDM_add = config['API']['MDM_add']
MDM_user = config['API']['MDM_username']
MDM_password = config['API']['MDM_password']
HES_add = config['API']['HES_add']
HES_user = config['API']['HES_username']
HES_password = config['API']['HES_password']

print("***** Where do you want to get data from HES or MDM *****")
while True:
    data_source = input("Enter the data source (HES or MDM): ").strip()

    if data_source == "HES":
        add = HES_add
        user = HES_user
        password = HES_password
        break
    elif data_source == "MDM":
        add = MDM_add
        user = MDM_user
        password = MDM_password
        break
    else:
        print("Invalid choice. Please enter either 'HES' or 'MDM'.")
        


print(f"Would you like to export data for all devices in {data_source} or only for devices listed in the CSV file?")
while True:
    print(f"Press 1 to export data for all devices in {data_source}.")
    print(f"Press 2 to export data for devices from the CSV file {data_source}.")
    dev_type = input("Input 1 or 2 and press Enter: ")
    if dev_type == '1':
        break
    elif dev_type == '2':
        dev_file_path = input("Enter the file name like 'device_list.csv': ")
        break
    else:
        print("Invalid choice. Please enter either '1' or '2'.")


def csv_to_device(filepath):
    dev_list = []
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if row:
                    dev_list.append(list(row.values())[0])
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
    except IOError:
        print(f"Error: An I/O error occurred while reading the file '{filepath}'.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return dev_list


def billing_time_data(month: str = None, year: int = None, start_hour: int = 18, start_minute: int = 29):
    # Use current year and month if not provided
    now = datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.strftime('%B')
    
    month = month.capitalize()
    cur_start_date = datetime(year, list(calendar.month_name).index(month), 
                              calendar.monthrange(year, list(calendar.month_name).index(month))[1],
                              start_hour, start_minute)
    start_time_cur = cur_start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_cur = (cur_start_date + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")        
    return start_time_cur, end_time_cur


def daily_time_data(day: int = None, month: str = None, year: int = None, start_hour: int = 18, start_minute: int = 29):
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
    else:
        date_obj = datetime(year, list(calendar.month_name).index(month), day) - timedelta(days=1)
        year, month, day = date_obj.year, date_obj.strftime('%B'), date_obj.day
    cur_start_date = datetime(year, list(calendar.month_name).index(month), day, start_hour, start_minute)
    start_time_cur = cur_start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_cur = (cur_start_date + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    return start_time_cur, end_time_cur


while True:
    profile = input("Enter the profile you want to extract data for (Billing or Daily): ").strip().lower()
    if profile == "billing":
        prof = "1-0:98.1.0*255"
        tprof = "Billing Profile"
        while True:
            try:
                month_input = input("Enter the month (e.g., January) or press Enter to use the current month: ")
                year_input = input("Enter the year (e.g., 2024) or press Enter to use the current year: ")
                month = month_input if month_input else None
                year = int(year_input) if year_input else None
                start_time_cur, to_time_cur = billing_time_data(month, year)
                break
            except (ValueError, IndexError):
                print("Invalid date. Please try again.")
        break
    elif profile == "daily":
        prof = "1-0:99.2.0*255"
        tprof = "Daily Profile"
        while True:
            try:
                year_input = input("Enter the year (e.g., 2024) or press Enter to use the current year: ")
                month_input = input("Enter the month (e.g., January) or press Enter to use the current month: ")
                day_input = input("Enter the day (e.g., 31) or press Enter to use today's date: ")
                month = month_input if month_input else None
                year = int(year_input) if year_input else None
                day = int(day_input) if day_input else None
                start_time_cur, to_time_cur = daily_time_data(day, month, year)
                break
            except (ValueError, IndexError):
                print("Invalid Date. Please try again")
        break
            
    else:
        print("Invalid choice. Please enter either 'billing' or 'daily'.")
        
end_date_part = start_time_cur.split("T")[0]

print("Data Will Be Extracted For Following: ")
print("======================================")
print(f"Data Source   : {data_source}")
print(f"Profile       : {tprof}")
print(f"Profile Value : {prof}")
print(f"Start Time    : {start_time_cur}")
print(f"End Time      : {to_time_cur}")
if dev_type == '1':
    print(f"Devices      : All Devices in {data_source}")
else:
    print(f"Devices      : All Devices in the {dev_file_path} file")

def get_devices(add, user, password):
    url= f"{add}/api/1/devices/"
    try:
        r = requests.get(url, auth=(MDM_user, MDM_password), verify=False)
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

def get_profile_data(address, username, password, profile, devlist, from_time, to_time):
    m_values_kwh = []
    m_values_kvah = []
    for device in devlist:
        device_id = device.get('id', 'N/A')
        groupName = device.get('groupName', 'N/A')
        url = f"{address}/api/1/devices/{device_id}/profiles/{profile}/entries?from={from_time}&to={to_time}"
        try:
            r = requests.get(url, auth=(username, password), verify=False)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            continue
        response_dict = r.json()
        entries = response_dict.get("entries", [])
        if entries:
            first_entry = entries[0]
            capturedAt = first_entry.get("capturedAt", [])
            metered_value = first_entry.get("meteredValues", [])

            for item in metered_value:
                if item["registerId"] == "1-0:1.8.0*255":
                    item["device"] = device_id
                    item["profile"] = profile
                    item["groupName"] = groupName
                    if item["measuredAt"] is None:
                        item["measuredAt"] = capturedAt
                    m_values_kwh.append(item)
                elif item["registerId"] == "1-0:9.8.0*255":
                    item["device"] = device_id
                    item["profile"] = profile
                    item["groupName"] = groupName
                    if item["measuredAt"] is None:
                        item["measuredAt"] = capturedAt
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




dev_list = get_devices(MDM_add, MDM_user, MDM_password)
if dev_type == '2':
    cdev = csv_to_device(dev_file_path)
    filtered_dev_list = []
    for item in dev_list:
        if item.get('id') in cdev:
            filtered_dev_list.append(item)
    dev_list = filtered_dev_list


write_dicts_to_csv(dev_list, f"{data_source}_Device_List_{end_date_part}.csv")
print(f"Starting Extracting Data for Current Month {len(dev_list)} Devices at: {datetime.now()}")
value_curr_kwh, value_curr_kvah = get_profile_data(add, user, password, prof, dev_list, start_time_cur, to_time_cur)


write_dicts_to_csv(value_curr_kwh,  f"{data_source}_value_kwh_{end_date_part}.csv")
write_dicts_to_csv(value_curr_kvah, f"{data_source}_value_kvah_{end_date_part}.csv")

data_avail=check_data_availability(dev_list, value_curr_kwh)
write_dicts_to_csv(data_avail,  f"{data_source}_data_avail_{end_date_part}.csv")
print("Data Extraction Complete")