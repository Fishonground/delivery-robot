from hashlib import sha256
import json
import multiprocessing
from flask import Flask, request, jsonify
from uuid import uuid4
import threading

host_name = "0.0.0.0"
port = 6006

app = Flask(__name__)             # create an app instance


_requests_queue: multiprocessing.Queue = None

@app.route("/pincoding", methods=['POST'])
def pincoding():
    content = request.json

    print(content)
    req_id = uuid4().__str__()

    try:
        hmi_details = {
            "id": req_id,
            "operation": "pincoding",
            "deliver_to": "central",
            "source":"hmi",
            "pincode": content['pincode'],
            "authorized": False,
            }
        _requests_queue.put(hmi_details)
        print(f"pincoding event: {hmi_details}")
    except:
        error_message = f"malformed request {request.data}"
        print(error_message)
        return error_message, 400
    return jsonify({"operation": "pincoding requested", "id": req_id})

def start_rest(requests_queue):
    global _requests_queue 
    _requests_queue = requests_queue
    threading.Thread(target=lambda: app.run(host=host_name, port=port, debug=True, use_reloader=False)).start()

if __name__ == "__main__":        # on running python app.py
    start_rest()