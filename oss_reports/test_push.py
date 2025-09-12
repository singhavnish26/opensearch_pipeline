#!/usr/bin/env python3
import requests
import urllib3
from opensearchpy import OpenSearch
from opensearch_helper import OSWriter  # <- your new v2 helper file
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Build the client OUTSIDE the helper
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    http_auth=("admin", "admin"),  # or pull from env/secrets
    use_ssl=False,                 # True if your node is HTTPS
    verify_certs=False             # True + ca_certs=... if you have a real CA
)
writer = OSWriter(client)




docs = [
    {
        "deviceId": "A1",
        "group": "Panchet",
        "measuredAt": "2025-08-31T18:30:00Z",
        "kwh": 12.5,
        "eventEpoch": 1725139200        # seconds (v2 won't auto-date this; see note)
    },
    {
        "deviceId": "A2",
        "group": "Panchet",
        "measuredAt": "2025-09-01 10:20:30",
        "kwh": 7,                        # will map to integer (long)
        "eventEpoch": 1725225600123      # millis
    },
]

index_name = "demo-auto-map"
writer.push(index_name=index_name, docs=docs, id_field="deviceId")
print(f"OK: pushed {len(docs)} docs into {index_name}")

