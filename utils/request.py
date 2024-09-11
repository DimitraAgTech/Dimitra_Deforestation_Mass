import concurrent.futures
import json
import os
import time
import uuid
from itertools import batched

import requests

from config import DEFORESTATION_API, NODE_CALLBACK_URL, SYNC_KEY
from constants import BATCH_SIZE, FAILED, PENDING, WORKERS
from database import db, models
from utils.logger import logger
from utils.s3 import download_s3_object, upload_s3_object
from utils.timer import time_it

headers = {"Auth-Token": SYNC_KEY, "Content-Type": "application/json"}


def get_available_mass_request():
    request_id = os.getenv("REQUEST_ID")

    query = db.session.query(models.DeforestationRequest)
    try:
        if request_id:
            logger.info(f"Request ID found in env : {request_id}")
            mass_request = (
                query.filter_by(id=uuid.UUID(request_id)
                                ).with_for_update().first()
            )
            return mass_request

        logger.info(f"No request ID found in env")
        mass_request = query.filter_by(
            status=PENDING).with_for_update().first()
        if not mass_request:
            mass_request = query.filter_by(
                status=FAILED).with_for_update().first()
    except Exception as e:
        logger.error(e)
        db.session.rollback()
        return None

    return mass_request


def update_mass_request(
    mass_request,
    status=None,
    error=None,
    is_synced=None,
    completed=None,
    timestamp=None,
    completion_timestamp=None,
):
    mass_request.status = status if status != None else mass_request.status
    mass_request.error = error if error != None else mass_request.error
    mass_request.is_synced = is_synced if is_synced != None else mass_request.is_synced
    mass_request.completed = completed if completed != None else mass_request.completed
    mass_request.timestamp = timestamp if timestamp != None else mass_request.timestamp
    mass_request.completion_timestamp = (
        completion_timestamp
        if completion_timestamp != None
        else mass_request.completion_timestamp
    )
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
    try:
        response = requests.post(
            f"{DEFORESTATION_API}/detect-deforestation-bulk", json=data, headers=headers
        )
    except Exception as e:
        logger.error(f"Error in make_deforestation_request : {e}")
        time.sleep(20)
        try:
            response = requests.post(
                f"{DEFORESTATION_API}/detect-deforestation-bulk", json=data, headers=headers
            )
        except Exception as e:
            logger.error(f"Error again in make_deforestation_request : {e}")
            return None

    try:
        return response.json()
    except Exception as e:
        logger.info(f"Error data : {data}")
        logger.error(f"Error in json : {e}")
        return None


def make_items_chunk_requests(items_chunk, options):
    # items chunk will have 50 items
    # divide these into another chunked list of 10 items each and 5 chunks
    chunks = get_chunked_items(items_chunk, chunk_size=BATCH_SIZE)
    chunks_with_options = [{**options, "items": chunk} for chunk in chunks]

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        results = list(executor.map(
            make_deforestation_request, chunks_with_options))

    return results


def notify_callback(request_id):
    data = {"request_id": str(request_id)}
    try:
        requests.post(NODE_CALLBACK_URL, json=data)
        return True
    except:
        return False
