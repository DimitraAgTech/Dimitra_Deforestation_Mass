import traceback
from datetime import datetime

from constants import BATCH_SIZE, COMPLETED, FAILED, IN_PROGRESS, WORKERS
from utils.logger import logger
from utils.request import (get_available_mass_request, get_chunked_items,
                           get_request_data, make_items_chunk_requests,
                           notify_callback, update_mass_request, upload_data)


def get_items_from_results(results):
    result_items = []
    for result in results:
        if result:
            result_items += result['data']

    return result_items


def run_mass_request(mass_request):
    logger.info(f"Processing request_id : {mass_request.id} with status : {mass_request.status}")
    logger.info(f"Using batch size : {BATCH_SIZE}")
    logger.info(f"Using workers : {WORKERS}")

    request_data = get_request_data(mass_request.id)

    mass_request = update_mass_request(mass_request, status=IN_PROGRESS)

    items = request_data.get('items')
    options = {**request_data, "items": None}

    if not items:
        update_mass_request(
            mass_request, status=FAILED, error="items not found in the request")
        return

    logger.info(f"Total items : {mass_request.total}")
    chunked_items = get_chunked_items(items, BATCH_SIZE*WORKERS)
    total_chunks = len(chunked_items)

    data = []
    for i, items_chunk in enumerate(chunked_items):
        logger.info(f"Processing chunk : {i+1}/{total_chunks}")

        results = make_items_chunk_requests(items_chunk, options)

        data += get_items_from_results(results)
        mass_request = update_mass_request(mass_request, completed=len(data))

        logger.info(f"Completed : {mass_request.completed}/{mass_request.total}")

    upload_data(mass_request.id, data)
    update_mass_request(mass_request, status=COMPLETED,
                        completion_timestamp=datetime.now())

    success = notify_callback(mass_request.id)
    error = "Notify callback api failed" if not success else None
    update_mass_request(mass_request, is_synced=success, error=error)


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
