import datetime
import requests
import json
import urllib3
import configparser
from opensearchpy import OpenSearch, exceptions


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
print(f"Script started at: {datetime.datetime.now()}")
config.read('/mnt/c/Users/singh/Documents/python_rakathon/opensearch_pipeline/credentials.ini')
MDMadd = config['API']['MDM_add']
user = config['API']['username']
password = config['API']['password']
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
