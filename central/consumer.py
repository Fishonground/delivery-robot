# implements Kafka topic consumer functionality

from datetime import datetime
import multiprocessing
import threading
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
from producer import proceed_to_deliver
import base64


_requests_queue: multiprocessing.Queue = None
PIN = 0
target_x = 0
target_y = 0
pincode_comp = False
attempts = 3
destination_point = False
epsilon = 1000
def compare_coords(coords):
    global target_x , target_y
    if abs(coords[0] - target_x) < epsilon and abs(coords[1] - target_y) < epsilon:
        if not destination_point:
            print("Deviation wasn't detected")
    else:
        print("Deviation was Detected!!!!")


def handle_event(id, details_str):
    details = json.loads(details_str)
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    global PIN
    global target_x
    global target_y
    global pincode_comp
    global attempts
    global destination_point
    try:
        if details['operation'] == 'ordering':
            attempts = 3
            PIN = details['pincode']
            target_x = details['x']
            target_y = details['y']
            print(f"[central] event {id}, order to {details['x']} : {details['y']} in working")

            global _requests_queue 
            confirmation_details = {
            "id": id,
            "operation": "confirmation",
            "deliver_to": "crypto",
            "source": "",
            "confirmation": True
            }
            _requests_queue.put(confirmation_details)

            details = {
            "id": id,
            "source": "",
            "operation": "count_direction",
            "deliver_to": "positioning",
            "direction": 0,
            "speed": 0,
            "distance": 0,
            "time": 0,
            "x": details['x'],
            "y": details['y']
            }
            print(f"[central] event {id}, order is counting")
            proceed_to_deliver(id, details)


        elif details['operation'] == 'count_direction':
            print(f"[central] event {id}, order is counted")
            details = {
            "id": id,
            "source": "",
            "operation": "motion_start",
            "deliver_to": "motion",
            "direction": details['direction'],
            "speed": details['speed'],
            "distance": details['distance'],
            "time": 0,
            "x": 0,
            "y": 0
            }
            print(f"[central] event {id}, robot started it's way")
            proceed_to_deliver(id, details)

        elif details['operation'] == 'pincoding':
            if destination_point:
                print(f"[central] event {id}, checking PIN")
                global pincode_comp
                if attempts > 0:
                    attempts-=1
                    if PIN == details['pincode']: 
                        pincode_comp = True
                        details['deliver_to'] = 'sensors'
                        details['operation'] = 'lock_opening'
                        proceed_to_deliver(id, details)
                        print(f"[central] event {id}, opening locks")
                    else: 
                        pincode_comp = False
                        print(f"[central] event {id}, wrong PIN detected!")
                else:
                    print(f"[central] event {id}, order was blocked")
                    details = {
                        "id": id,
                        "source": "",
                        "operation": "count_direction",
                        "deliver_to": "positioning",
                        "direction": 0,
                        "speed": 0,
                        "distance": 0,
                        "time": 0,
                        "x": 0,
                        "y": 0
                    }
                    print(f"[central] event {id}, way back is counting")
                    proceed_to_deliver(id, details)
        
        elif details['operation'] == 'stop':

            if (round(details['x'],-1) == 0) and (round(details['y'],-1) == 0):
                details = {
                    "id": id,
                    "source": "",
                    "operation": "ready",
                    "deliver_to": "crypto"
                    }
                proceed_to_deliver(id, details)
            else:
                if (round(details['x'],-1) == round(target_x,-1)) and (round(details['y'],-1) == round(target_y,-1)):
                    destination_point = True
                    print(f"[central] event {id}, robot is in destination point, ready for PIN!")
                else:
                    print(f"[error] event {id}, robot is in not correct position! Expected {target_x} : {target_y} but received {details['x']} : {details['y']}")
        
        elif details['operation'] == 'lock_closing':
            details = {
            "id": id,
            "source": "",
            "operation": "count_direction",
            "deliver_to": "positioning",
            "direction": 0,
            "speed": 0,
            "distance": 0,
            "time": 0,
            "x": 0,
            "y": 0
            }
            print(f"[central] event {id}, way back is counting")
            proceed_to_deliver(id, details)
        elif details['operation'] == "set_coords":
            coords = details['coords']
            compare_coords(coords)
        else:
            print(f"[warning] unknown operation!\n{details}")                
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    


def consumer_job(args, config, requests_queue: multiprocessing.Queue):
    # Create Consumer instance
    central_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(central_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            central_consumer.assign(partitions)

    # Subscribe to topic
    topic = "central"
    central_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = central_consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"[error] {msg.error()}")
            else:
                try:
                    id = msg.key().decode('utf-8')
                    details_str = msg.value().decode('utf-8')
                    handle_event(id, details_str)
                except Exception as e:
                    print(
                        f"[error] Malformed event received from topic {topic}: {msg.value()}. {e}")
    except KeyboardInterrupt:
        pass
    finally:
        central_consumer.close()


def start_consumer(args, config, requests_queue):
    global _requests_queue
    _requests_queue = requests_queue
    threading.Thread(target=lambda: consumer_job(args, config, requests_queue)).start()


if __name__ == '__main__':
    start_consumer(None)
