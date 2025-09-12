import time
import json
import urllib3
import requests
import logging
from datetime import datetime
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter  
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="opensearchpy")

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="device_status.log",  # Log file path
    filemode="a"  
)
logger = logging.getLogger(__name__)


# Configuration
MDMSaddress = "http://100.102.4.10:8081/zonos-api"
muser, msecret = "mdm_dvc_admin", "Hb1VNBRD8WLAu27B"
url = f"{MDMSaddress}/api/1/devices"
threshold = 172800  # 48 hours
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


#Fetch device status from MDM
while True:
    try:
        logger.debug("Sending request to MDM API: %s", url)
        r = requests.get(url, verify=False, auth=(muser, msecret))
        r.raise_for_status()
        logger.info("Successfully fetched data from MDM API")
        break
    except requests.exceptions.RequestException as e:
        logger.error("API error: %s", e)
        time.sleep(5)
        continue

returnJSON = json.loads(r.content.decode("utf-8"))
logger.debug("Received JSON response: %s", returnJSON)

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
logger.info("Processed %d devices", len(dev_list))


client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),  
    use_ssl=False,                 
    verify_certs=False             
)
logger.debug("OpenSearch client initialized")

writer = OSWriter(client)
index_name = "device-status" + datetime.now().strftime("%y-%m")
logger.debug("Index name: %s", index_name)

writer.push(index_name=index_name, docs=dev_list, id_field="deviceId")
logger.info("OK: pushed %d docs into %s", len(dev_list), index_name)
