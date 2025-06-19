import os
import json
import threading
import multiprocessing

from confluent_kafka import Producer


_requests_queue: multiprocessing.Queue = None


def proceed_to_deliver(id, details):
    details["id"] = id
    details["source"] = 'complex'
    _requests_queue.put(details)


def producer_job(_, config, requests_queue: multiprocessing.Queue):
    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print("[error] Message failed delivery: {}".format(err))

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
        producer.flush()


def start_producer(args, config, requests_queue):
    global _requests_queue

    _requests_queue = requests_queue
    threading.Thread(
        target=lambda: producer_job(args, config, requests_queue)
    ).start()