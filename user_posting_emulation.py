from awsdb import AWSDBConnector
from emulation_functions import EmulationFunctions
import random
from time import sleep

new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    ''' Runs an infinite loop in 0-2 second intervals, fetching json data from the pinterest database to MSK clusters '''
    while True:
        sleep(random.randrange(0, 2))
        engine = new_connector.create_db_connector()
        db_creds = new_connector.db_creds
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

        with engine.connect() as connection:
            emulation_functions = EmulationFunctions(headers, connection, db_creds)
            
            ''' Get json data for pin, geo and user tables and post to MSK clusters '''
            emulation_functions.get_json_data('pin', 'pinterest_data')
            emulation_functions.get_json_data('geo', 'geolocation_data')
            emulation_functions.get_json_data('user', 'user_data')

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    