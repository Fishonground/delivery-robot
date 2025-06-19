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
timestamp = time.time()
speed = 0
direction = 0
#stopped = 0

def move_move(id, details, journey_time):
    while True:
        time.sleep(journey_time-0.1)
        print(f"[motion] robot seems went all way")
        now = time.time()
        details = {
                    "id": id,
                    "source": "",
                    "operation": "stop",
                    "deliver_to": "positioning",
                    "direction": details['direction'],
                    "speed": 0,
                    "distance": speed * (now - timestamp),
                    "time": now - timestamp,
                    "x": 0,
                    "y": 0
                    }
        print(f"[motion] event {id}, stopped in {now}, went {details['distance']}")
        proceed_to_deliver(id, details)
        break
        
        

def handle_event(id, details_str):
    details = json.loads(details_str)
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    try:
        delivery_required = False
        if details['operation'] == 'motion_start':
            if details['speed'] >= 5: details['speed'] = 5
            journey_time = abs(details['distance'] / details['speed']) 
            global timestamp
            global speed
            global direction
            global stopped
            stopped = False
            timestamp = time.time();
            speed = details['speed']
            direction = details['direction']
            print(f"[motion] event {id}, will take {journey_time} from {timestamp}")
            details = {
            "id": id,
            "source": "",
            "operation": "motion_start",
            "deliver_to": "positioning",
            "direction": details['direction'],
            "speed": details['speed'],
            "distance": 0,
            "time": timestamp + journey_time
            }
            threading.Thread(target=lambda: move_move(id, details, journey_time)).start()
            delivery_required = True
        elif details['operation'] == 'stop':
            print("How did we get here?")
            delivery_required = True
        else:
            print(f"[warning] unknown operation!\n{details}")                
        if delivery_required:
            proceed_to_deliver(id, details)
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    


def consumer_job(args, config):
    # Create Consumer instance
    motion_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(motion_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            motion_consumer.assign(partitions)

    # Subscribe to topic
    topic = "motion"
    motion_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = motion_consumer.poll(1.0)
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
        motion_consumer.close()


def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()


if __name__ == '__main__':
    start_consumer(None)
