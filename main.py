import traceback
import uuid
from datetime import datetime

from config import BATCH_SIZE, WORKERS
from constants import COMPLETED, FAILED, IN_PROGRESS
from report_generator.generator import generate_report_and_upload_data
from utils.logger import logger
from utils.request import (generate_data, get_available_mass_request,
                           get_request_data, notify_callback,
                           update_mass_request, upload_data)


def get_request_time_taken(mass_request):
    time_delta = mass_request.completion_timestamp - mass_request.timestamp

    if time_delta.seconds > 300:
        time_taken = f"{time_delta.seconds // 60} min."
    else:
        time_taken = f"{time_delta.seconds} sec."

    return time_taken


def insert_id_in_items(items):
    return [{"id": uuid.uuid4(), **item} for item in items]


def get_item_id_map(items):
    return {item.get("id"): item for item in items}


def run_mass_request(mass_request):
    logger.info(f"Processing request_id : {
                mass_request.id} with status : {mass_request.status}")
    logger.info(f"Using batch size : {BATCH_SIZE}")
    logger.info(f"Using workers : {WORKERS}")

    request_data = get_request_data(mass_request.id)

    mass_request = update_mass_request(mass_request, status=IN_PROGRESS)

    items = request_data.get('items')
    generate_report = request_data.get('generateReport', False)
    invoke_callback = request_data.get('invokeCallback', True)
    options = {**request_data, "items": None}

    if not items:
        update_mass_request(
            mass_request, status=FAILED, error="items not found in the request")
        return

    items = insert_id_in_items(items)
    data, mass_request = generate_data(mass_request, items, options)

    if not generate_report:
        upload_data(mass_request.id, data)
    else:
        item_id_map = get_item_id_map(items)
        generate_report_and_upload_data(mass_request.id, item_id_map, data)

    mass_request = update_mass_request(mass_request, status=COMPLETED,
                                       completion_timestamp=datetime.now())

    if invoke_callback:
        success = notify_callback(mass_request.id)
        error = "Notify callback api failed" if not success else None
        mass_request = update_mass_request(
            mass_request, is_synced=success, error=error)

    logger.info(f"Total time taken : {get_request_time_taken(mass_request)}")


def main():
    mass_request = get_available_mass_request()
    if not mass_request:
        logger.info("No PENDING requests found, quitting the service")
        return

    try:
        run_mass_request(mass_request)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(e)
        update_mass_request(mass_request, is_synced=False,
                            status=FAILED, error=str(e))


main()
