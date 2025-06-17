from hashlib import sha256
import multiprocessing
from flask import Flask, request, jsonify
from uuid import uuid4
import threading

host_name = "0.0.0.0"
port = 6008

app = Flask(__name__)             # create an app instance


_requests_queue: multiprocessing.Queue = None

@app.route("/ordering", methods=['POST'])
def ordering():
    content = request.json
    auth = request.headers['auth']
    if auth != 'very-secure-token':
        return "unauthorized", 401

    req_id = uuid4().__str__()

    try:
        communication_details = {
            "id": req_id,
            "operation": "ordering",
            "deliver_to": "crypto",
            "source": "",
            #"pincode": sha256(content['pincode']).hexdigest(),
            "pincode": content['pincode'],
            "x": content['x'],
            "y": content['y']
            }
        _requests_queue.put(communication_details)
        print(f"ordering event: {communication_details}")
    except:
        error_message = f"malformed request {request.data}"
        print(error_message)
        return error_message, 400
    return jsonify({"operation": "ordering requested", "id": req_id}),200

def start_rest(requests_queue):
    global _requests_queue 
    _requests_queue = requests_queue
    threading.Thread(target=lambda: app.run(host=host_name, port=port, debug=True, use_reloader=False)).start()

if __name__ == "__main__":        # on running python app.py
    start_rest()