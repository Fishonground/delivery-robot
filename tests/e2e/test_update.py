from time import sleep
import pytest
import re
import json
from urllib.request import urlopen, Request
from uuid import uuid1
import requests
import subprocess

COMMUNICATION_URL = "http://localhost:6008/ordering"
HMI_URL = "http://localhost:6006/pincoding"

headers = {
    "Content-Type": "application/json",
    "auth": "very-secure-token"
}
payload = {
    "pincode": 1234,
    "x": 12,
    "y": 12
}

pin_payload = {
    "pincode": 1234
} 

@pytest.fixture
def create_order():
    response = requests.post(url = COMMUNICATION_URL,headers=headers,json=payload)
    return response

@pytest.fixture
def get_logs():
    def _get_logs(container_name):
        return subprocess.check_output(
            ['docker-compose', 'logs', '--no-color', container_name],
            text=True,
        )
    return _get_logs

@pytest.fixture
def send_pin():
    response = requests.post(url = HMI_URL, headers=headers, json=pin_payload)
    return response
def test_correct_input():
    assert payload["pincode"] == pin_payload["pincode"]    

def test_order_response(create_order):
    response = create_order
    assert response.status_code == 200

def test_order_creation(get_logs):
    sleep(5)
    logs = get_logs('communication')
    expected_answer_for_order = f"'operation': 'ordering', 'deliver_to': 'crypto', 'source': '', 'pincode': {str(payload['pincode'])}, 'x': {str(payload['x'])}, 'y': {str(payload['y'])}"
    assert expected_answer_for_order in logs

def test_prePIN(get_logs):
    sleep(40)
    logs = get_logs('central')
    expected_answer_for_pin = "robot is in destination point, ready for PIN!"
    assert expected_answer_for_pin in logs

def test_pin_request(send_pin):
    response = send_pin
    assert response.status_code == 200
    
def test_finish(get_logs):
    sleep(20)
    logs = get_logs('central')
    expected = "'source': 'central', 'operation': 'ready', 'deliver_to': 'crypto'}"
    assert expected in logs