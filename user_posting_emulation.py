import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from awsdb import AWSDBConnector


random.seed(100)


class AWSDBConnector:
    def read_db_creds(self):
        """ Reads database credentials from db_creds.yaml and returns a dictionary
        """
        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return(db_creds)

    def __init__(self):
        self.db_creds = self.read_db_creds()
        self.HOST = self.db_creds['HOST']
        self.USER = self.db_creds['USER']
        self.PASSWORD = self.db_creds['PASSWORD']
        self.DATABASE = self.db_creds['DATABASE']
        self.PORT = self.db_creds['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            pin_url = new_connector.db_creds['PIN_URL']
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
            pin_data = json.dumps({
                "records": [
                    {
                        "value": {
                            "index": pin_result["index"], 
                            "unique_id": pin_result["unique_id"], 
                            "title": pin_result["title"], 
                            "description": pin_result["description"], 
                            "poster_name": pin_result["poster_name"], 
                            "follower_count": pin_result["follower_count"], 
                            "tag_list": pin_result["tag_list"], 
                            "is_image_or_video": pin_result["is_image_or_video"], 
                            "image_src": pin_result["image_src"], 
                            "downloaded": pin_result["downloaded"], 
                            "save_location": pin_result["save_location"], 
                            "category": pin_result["category"]
                        }
                    }
                ]
            })

            print(pin_data)
            pin_response = requests.request("POST", pin_url, headers=headers, data=pin_data)
            print(pin_response.status_code)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            geo_url = new_connector.db_creds['GEO_URL']
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            geo_data = json.dumps({
                "records": [
                    {
                        "value": {
                            "ind": geo_result["ind"], 
                            "timestamp": geo_result["timestamp"].strftime("%m/%d/%Y, %H:%M:%S"), 
                            "latitude": geo_result["latitude"], 
                            "longitude": geo_result["longitude"], 
                            "country": geo_result["country"]
                        }
                    }
                ]
            })
            print(geo_data)
            geo_response = requests.request("POST", geo_url, headers=headers, data=geo_data)
            print(geo_response.status_code)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            user_url = new_connector.db_creds['USER_URL']
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

            user_data = json.dumps({
                "records": [
                    {
                        "value": {
                            "ind": user_result["ind"], 
                            "first_name": user_result["first_name"], 
                            "last_name": user_result["last_name"], 
                            "age": user_result["age"], 
                            "date_joined": user_result["date_joined"].strftime("%m/%d/%Y, %H:%M:%S")
                        }
                    }
                ]
            })

            print(user_data)
            user_response = requests.request("POST", user_url, headers=headers, data=user_data)
            print(user_response.status_code)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


