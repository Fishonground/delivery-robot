import os
import json
import threading

from uuid import uuid4
from time import sleep
from confluent_kafka import Consumer, OFFSET_BEGINNING

from producer import proceed_to_deliver

coords = {
    "internal": [0, 0],
    "gnss": [0, 0]
}

def avrg_coords(coords):
    return [(coords["internal"][i] + coords["gnss"][i]) // 2 for i in range(2)]


def check():
    while True:
        global coords
        print("[COMPLEX_DEBUG] Set coords:", avrg_coords(coords))

        proceed_to_deliver(uuid4().__str__(), {
            "deliver_to": "central",
            "operation": "set_coords",
            "coords": avrg_coords(coords)
        })

        sleep(15)


def handle_event(id, details_str):
    """ Имитатор работы блока комплексирования. """
    details = json.loads(details_str)

    source: str = details.get("source")
    deliver_to: str = details.get("deliver_to")
    operation: str = details.get("operation")

    print(f"[info] handling event {id}, "
          f"{source}->{deliver_to}: {operation}")

    new_coords = details.get("coords")
    if not new_coords:
        return

    global coords

    if operation == "set_internal_coords":
        coords["internal"] = new_coords
    elif operation == "set_gnss_coords":
        coords["gnss"] = new_coords


def consumer_job(args, config):
    consumer = Consumer(config)

    def reset_offset(verifier_consumer, partitions):
        if not args.reset:
            return

        for p in partitions:
            p.offset = OFFSET_BEGINNING
        verifier_consumer.assign(partitions)

    topic = 'complex'
    consumer.subscribe([topic], on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)
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
                    print(f"[error] Malformed event received from " \
                          f"topic {topic}: {msg.value()}. {e}")
    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()
    threading.Thread(target=check).start()