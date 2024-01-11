import json
import requests
import random
from sqlalchemy import text
import datetime

random.seed(100)

class EmulationFunctions:
    def __init__(self, headers, connection, db_creds):
        self.headers = headers
        self.connection = connection
        self.db_creds = db_creds
        self.url = self.db_creds['URL']
        self.streaming_url = db_creds['STREAMING_URL']
        self.keys = {
            "pin": ['index', 'unique_id', 'title', 'description', 'poster_name', 'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 'downloaded', 'save_location', 'category'],
            "geo": ['ind', 'timestamp', 'latitude', 'longitude', 'country'],
            "user": ['ind', 'first_name', 'last_name', 'age', 'date_joined']
        }
    
    def get_random_row(self):
        return random.randint(0, 11000)
    
    def get_json_data(self, table_name, data_source):
        random_row = self.get_random_row()
        string = text(f"SELECT * FROM {data_source} LIMIT {random_row}, 1")
        selected_row = self.connection.execute(string)
        keys = self.keys[table_name]
        url = self.url + table_name

        for row in selected_row:
            result = dict(row._mapping)
            value = {key: (result[key].strftime("%m/%d/%Y, %H:%M:%S") if isinstance(result[key], datetime.date) else result[key]) for key in keys }
            data = json.dumps({
                "records": [
                    {
                        "value": value
                    }
                ]
            })

            print(data)
            response = requests.request("POST", url, headers=self.headers, data=data)
            print(response.status_code)
    
    def get_streaming_data(self, table_name, data_source):
        random_row = self.get_random_row()
        string = text(f"SELECT * FROM {data_source} LIMIT {random_row}, 1")
        selected_row = self.connection.execute(string)
        keys = self.keys[table_name]
        url = self.streaming_url + table_name + '/record'

        for row in selected_row:
            result = dict(row._mapping)
            value = {key: (result[key].strftime("%m/%d/%Y, %H:%M:%S") if isinstance(result[key], datetime.date) else result[key]) for key in keys }
            data = json.dumps({
                "StreamName": "streaming-124df56aef51-pin",
                "Data": 
                    #Data should be send as pairs of column_name:value, with different columns separated by commas 
                    value,
                "PartitionKey": f"{table_name}-data"
            })

            print(data)
            response = requests.request("PUT", url, headers=self.headers, data=data)
            print(response.status_code)