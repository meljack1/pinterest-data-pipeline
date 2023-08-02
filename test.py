import requests
import json
from datetime import datetime

geo_result = {"ind": 2, "timestamp": datetime.now(), "latitude": 25, "longitude": 24, "country": "France"}

# invoke url for one record, if you want to put more records replace record with records
invoke_url = "https://cjjp70w740.execute-api.us-east-1.amazonaws.com/streaming/streams/streaming-124df56aef51-pin/record"

#To send JSON messages you need to follow this structure
payload = json.dumps({
    "StreamName": "streaming-124df56aef51-pin",
    "Data": {
            #Data should be send as pairs of column_name:value, with different columns separated by commas      
            "ind": geo_result["ind"], 
            "timestamp": geo_result["timestamp"].strftime("%m/%d/%Y, %H:%M:%S"), 
            "latitude": geo_result["latitude"], 
            "longitude": geo_result["longitude"], 
            "country": geo_result["country"]
            },
            "PartitionKey": "geo-name"
            })

headers = {'Content-Type': 'application/json'}

response = requests.request("PUT", invoke_url, headers=headers, data=payload)
print(response.status_code)
print(payload)