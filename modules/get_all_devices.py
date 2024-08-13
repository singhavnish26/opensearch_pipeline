import datetime
import requests
import json
import urllib3
from opensearchpy import OpenSearch, exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
print(f"Script started at: {datetime.datetime.now()}")

MDMadd = "https://zonos.dvc.engrid.in/zonos-api"
user = "sppl_admin"
password = "$ppli-@dm1n"
url = f"{MDMadd}/api/1/devices/"

def get_devices():
    try:
        r = requests.get(url, auth=(user, password), verify=False)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return []
    devices = r.json()
    return devices

devices=get_devices()
with open("/mnt/c/Users/singh/Documents/python_rakathon/opensearch_pipeline/datasets/all_devices.json", "w") as outfile:
    json.dump(devices, outfile)


"""def get_devices():
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
    return device"""