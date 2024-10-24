import concurrent.futures
import json
import os
import time
import traceback
import uuid
from itertools import batched

import requests

from config import (BATCH_SIZE, DEFORESTATION_API,
                    NODE_DEFORESTATION_CALLBACK_URL,
                    NODE_GEOFENCE_INFO_CALLBACK_URL, REQUEST_ID, SYNC_KEY,
                    TASK_TYPE, WORKERS)
from constants import (DEFORESTATION_TASK_TYPE, FAILED,
                       GEOFENCE_INFO_TASK_TYPE, PENDING)
from database import db, models
from utils.logger import logger
from utils.s3 import download_s3_object, upload_s3_object
from utils.timer import time_it

headers = {"Auth-Token": SYNC_KEY, "Content-Type": "application/json"}


def get_available_mass_request():
    if TASK_TYPE == DEFORESTATION_TASK_TYPE:
        query = db.session.query(models.DeforestationRequest)
    elif TASK_TYPE == GEOFENCE_INFO_TASK_TYPE:
        query = db.session.query(models.GeofenceInfoRequest)
    else:
        raise Exception("TASK_TYPE is incorrect")

    try:
        if REQUEST_ID:
            logger.info(
                f"Request ID found in env : {
                    REQUEST_ID} for Task type : {TASK_TYPE}"
            )
            mass_request = (
                query.filter_by(id=uuid.UUID(REQUEST_ID)
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


def get_items_from_results(results):
    result_items = []
    for result in results:
        if result:
            result_items += result["data"]

    return result_items


def get_request_data(request_id):
    if TASK_TYPE == DEFORESTATION_TASK_TYPE:
        s3_key = f"mass_deforestation/{request_id}/input.json"
    elif TASK_TYPE == GEOFENCE_INFO_TASK_TYPE:
        s3_key = f"mass_geofence_info/{request_id}/input.json"

    os.makedirs("temp", exist_ok=True)
    temp_path = os.path.join("temp", "input.json")

    download_s3_object(s3_key, temp_path)

    with open(temp_path) as f:
        return json.load(f)


def upload_data(request_id, data):
    if TASK_TYPE == DEFORESTATION_TASK_TYPE:
        s3_key = f"mass_deforestation/{request_id}/output.json"
    elif TASK_TYPE == GEOFENCE_INFO_TASK_TYPE:
        s3_key = f"mass_geofence_info/{request_id}/output.json"

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
    response = None
    try:
        response = requests.post(
            f"{DEFORESTATION_API}/detect-deforestation-bulk", json=data, headers=headers
        )
        result = response.json()
        if 'data' not in result:
            raise Exception("Invalid result from API, result has not data")
        return result
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(f"Error in make_deforestation_request : {e}")
        logger.error(f"Error response content : {response.content if response else response}")
        logger.error(f"Error data : {json.dumps(data)}")
        logger.error(f"Retrying request after 30 sec.")
        time.sleep(30)
        logger.error(f"Retrying this request")
        try:
            response = requests.post(
                f"{DEFORESTATION_API}/detect-deforestation-bulk",
                json=data,
                headers=headers,
            )
            result = response.json()
            if 'data' not in result:
                raise Exception("Invalid result from API, result has not data")
            return result
        except Exception as e:
            logger.error(f"Error again in make_deforestation_request : {e}")
            logger.error(f"Error response content : {response.content if response else response}")
            logger.error(f"Error data : {json.dumps(data)}")
            return None


@time_it
def make_geofence_info_request(data):
    response = None
    try:
        response = requests.post(
            f"{DEFORESTATION_API}/geofence-info-bulk", json=data, headers=headers)
        return response.json()
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(f"Error in make_geofence_info_request : {e}")
        logger.error(f"Error response content : {
                     response.content if response else response}")
        logger.error(f"Error data : {json.dumps(data)}")
        logger.error(f"Retrying request after 30 sec.")
        time.sleep(30)
        logger.error(f"Retrying this request")
        try:
            response = requests.post(
                f"{DEFORESTATION_API}/geofence-info-bulk",
                json=data,
                headers=headers,
            )
            return response.json()
        except Exception as e:
            logger.error(f"Error again in make_geofence_info_request : {e}")
            return None


def make_items_chunk_requests(items_chunk, options):
    chunks = get_chunked_items(items_chunk, chunk_size=BATCH_SIZE)
    chunks_with_options = [{**options, "items": chunk} for chunk in chunks]

    if TASK_TYPE == DEFORESTATION_TASK_TYPE:
        request_func = make_deforestation_request
    elif TASK_TYPE == GEOFENCE_INFO_TASK_TYPE:
        request_func = make_geofence_info_request
    else:
        raise Exception("Task type is invalid")

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        results = list(executor.map(request_func, chunks_with_options))

    return results


def notify_callback(request_id):
    data = {"request_id": str(request_id)}
    try:
        if TASK_TYPE == DEFORESTATION_TASK_TYPE:
            requests.post(NODE_DEFORESTATION_CALLBACK_URL, json=data)
        if TASK_TYPE == GEOFENCE_INFO_TASK_TYPE:
            requests.post(NODE_GEOFENCE_INFO_CALLBACK_URL, json=data)
        return True
    except:
        return False


def generate_data(mass_request, items, options):
    logger.info(f"Total items : {mass_request.total}")
    chunked_items = get_chunked_items(items, BATCH_SIZE * WORKERS)
    total_chunks = len(chunked_items)

    data = []
    for i, items_chunk in enumerate(chunked_items):
        logger.info(f"Processing chunk : {i+1}/{total_chunks}")

        results = make_items_chunk_requests(items_chunk, options)

        data += get_items_from_results(results)
        mass_request = update_mass_request(mass_request, completed=len(data))

        logger.info(f"Completed : {
                    mass_request.completed}/{mass_request.total}")

    return data, mass_request
