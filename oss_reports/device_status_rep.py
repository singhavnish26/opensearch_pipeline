import time
import json
import urllib3
import requests
from datetime import datetime
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter  


# Configuration
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
threshold = 172800  # 48 hours
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


#Fetch device status from MDM
while True:
    try:
        r = requests.get(url, verify=False, auth=(muser, msecret))
        r.raise_for_status()
        break
    except requests.exceptions.RequestException as e:
        print("API error:", e)
        time.sleep(5)
        continue

returnJSON = json.loads(r.content.decode("utf-8"))

dev_list = []
for item in returnJSON:
    payType = "Prepaid" if "prepaid" in item["groupName"].lower() else "Postpaid"
    last_connection = item["lastConnection"] if item["lastConnection"] is not None else 0
    delta = int(time.time()) - last_connection
    group = " ".join(w for w in item["groupName"].split() if w.lower() not in {"prepaid", "postpaid"})

    dev_list.append({
        "deviceId": item["id"],
        "group": group,
        "lastConnect": last_connection,
        "ingestTime": int(time.time()),
        "deltaTime": delta,
        "status": "Online" if delta < threshold else "Offline",
        "payType": payType,
        "InstalledState": item["inventoryState"],
        "@timestamp": datetime.utcnow().isoformat()
    })


client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),  # or pull from env/secrets
    use_ssl=False,                 # True if your node is HTTPS
    verify_certs=False             # True + ca_certs=... if you have a real CA
)
writer = OSWriter(client)

index_name = "device-status" + datetime.now().strftime("%y-%m")
writer.push(index_name=index_name, docs=dev_list, id_field="deviceId")
print(f"OK: pushed {len(dev_list)} docs into {index_name}")
