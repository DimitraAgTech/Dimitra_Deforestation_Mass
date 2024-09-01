import concurrent.futures
import json
import os
from itertools import batched

import requests

from config import DEFORESTATION_API, NODE_CALLBACK_URL, SYNC_KEY
from constants import INNER_CHUNKS, PENDING
from database import db, models
from utils.s3 import download_s3_object, upload_s3_object
from utils.timer import time_it

headers = {
    "Auth-Token": SYNC_KEY,
    "Content-Type": "application/json"
}


def get_pending_mass_request():
    try:
        mass_request = db.session.query(models.DeforestationRequest).filter_by(status=PENDING).with_for_update().first()
    except:
        db.session.rollback()
        return None        

    return mass_request


def update_mass_request(mass_request, status=None, error=None, is_synced=None, completed=None, completion_timestamp=None):
    mass_request.status = status if status != None else mass_request.status
    mass_request.error = error if error != None else mass_request.error
    mass_request.is_synced = is_synced if is_synced != None else mass_request.is_synced
    mass_request.completed = completed if completed != None else mass_request.completed
    mass_request.completion_timestamp = completion_timestamp if completion_timestamp!=None else mass_request.completion_timestamp
    db.session.commit()

    return mass_request


def get_request_data(request_id):
    s3_key = f"mass_deforestation/{request_id}/input.json"

    os.makedirs("temp", exist_ok=True)
    temp_path = os.path.join("temp", "input.json")

    download_s3_object(s3_key, temp_path)

    with open(temp_path) as f:
        return json.load(f)


def upload_data(request_id, data):
    s3_key = f"mass_deforestation/{request_id}/output.json"

    os.makedirs("temp", exist_ok=True)
    temp_path = os.path.join("temp", "output.json")

    with open(temp_path, "w") as f:
        json.dump(data, f)

    upload_s3_object(temp_path, s3_key)


def get_chunked_items(items, chunk_size=10):
    chunked_list = list(batched(items, chunk_size))
    return chunked_list


@time_it
def make_deforestation_request(data):
    response = requests.post(
        f"{DEFORESTATION_API}/detect-deforestation-bulk", json=data, headers=headers)

    return response.json()


def make_items_chunk_requests(items_chunk, options):
    # items chunk will have 50 items
    # divide these into another chunked list of 10 items each and 5 chunks
    chunks = get_chunked_items(items_chunk, chunk_size=INNER_CHUNKS)
    chunks_with_options = [{**options, "items": chunk} for chunk in chunks]

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        results = list(executor.map(
            make_deforestation_request, chunks_with_options))

    return results


def notify_callback(request_id):
    data = {"request_id": str(request_id)}
    requests.post(NODE_CALLBACK_URL, json=data)
