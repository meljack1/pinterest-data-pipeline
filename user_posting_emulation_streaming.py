from time import sleep
from awsdb import AWSDBConnector
import random
from emulation_functions import EmulationFunctions

random.seed(100)
new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        engine = new_connector.create_db_connector()
        db_creds = new_connector.db_creds
        headers = {'Content-Type': 'application/json'}

        with engine.connect() as connection:
            emulation_functions = EmulationFunctions(headers, connection, db_creds)

            # Get streaming data for pin, geo and user tables and post to connection
            emulation_functions.get_streaming_data('pin', 'pinterest_data')
            emulation_functions.get_streaming_data('geo', 'geolocation_data')
            emulation_functions.get_streaming_data('user', 'user_data')


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


