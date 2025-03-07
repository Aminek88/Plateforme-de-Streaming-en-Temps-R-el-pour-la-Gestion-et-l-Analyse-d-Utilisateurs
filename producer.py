#### create producer script.py for kafka producer 
import requests
import json
import time 
from kafka import KafkaProducer
from requests.exceptions import RequestException



### kafka producer setup 
producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092' , 
    value_serializer = lambda v:json.dumps(v).encode("utf-8")
)

###API and kafka setting 
API_URL = "https://randomuser.me/api/?results=10"
topic_name = 'userData'
fetch_intervalle=10 ### seconds between API call
Retry_Delay = 60 


def fetch_send_data():
    while True : 
        try : 
            reponse = requests.get(API_URL , timeout=5)
            reponse.raise_for_status() ## chech for API errors

            data=reponse.json()  ## get json data
            producer.send(topic_name , value=data)   ###send to kafka 
            producer.flush()
            print(f'send batch of 10 users to kafka topic :{topic_name}')

            time.sleep(fetch_intervalle)

        except RequestException as e : 
            print(f"API error : {e} retry in {Retry_Delay} s")
            time.sleep(Retry_Delay)
        except Exception as e : 
            print(f"Unexpected error {e} , retry in {Retry_Delay} s")
            time.sleep(Retry_Delay)


if __name__=='__main__':
    print('starting_kafka producer ')
    try :
        fetch_send_data() 
    except KeyboardInterrupt:
        print("\nshutting down kafka")
        producer.close()
        print('Producer stopped')

