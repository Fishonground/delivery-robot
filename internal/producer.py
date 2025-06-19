import os
import json
import threading
import multiprocessing

from uuid import uuid4
from time import sleep
from random import randint
from confluent_kafka import Producer

_requests_queue: multiprocessing.Queue = None


def generate_coordinates():
    while True:
        new_x: int = randint(-50,50)
        new_y: int = randint(-50,50)

        coords = [new_x , new_y]
        proceed_to_deliver(uuid4().__str__(),
                           {
                               "deliver_to": "complex",
                               "operation" : "set_internal_coords",
                               "coords": coords
                           })
        sleep(randint(10,15))
    
def proceed_to_deliver(id , details):
    details["id"] = id
    details["source"] = 'internal'
    _requests_queue.put(details)

def producer_job(_, config, requests_queue: multiprocessing.Queue):
    producer = Producer(config)

    threading.Thread(target=generate_coordinates).start()

    def delivery_callback(err, msg):
        if err:
            print("failed to deliver message{}".format(err))
    topic = "monitor"
    while True:
        event_details = requests_queue.get()
        producer.produce(
            topic,
            json.dumps(event_details),
            event_details["id"],
            callback=delivery_callback
        )
        producer.poll(10000)
        producer.flush

def start_producer(args , config, requests_queue):

    global _requests_queue

    _requests_queue = requests_queue

    threading.Thread(
        target=lambda: producer_job(args, config, requests_queue)).start()