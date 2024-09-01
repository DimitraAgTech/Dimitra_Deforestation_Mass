from constants import COMPLETED, FAILED, IN_PROGRESS, OUTER_CHUNKS
from datetime import datetime
from utils.logger import logger
from utils.request import (get_chunked_items, get_pending_mass_request,
                           get_request_data, make_items_chunk_requests,
                           notify_callback, update_mass_request, upload_data)


def get_items_from_results(results):
    result_items = []
    for result in results:
        result_items += result['data']

    return result_items


def main():
    mass_request = get_pending_mass_request()
    if not mass_request:
        logger.info("No PENDING requests found, quitting")
        return

    logger.info(f"Request ID : {mass_request.id}")

    request_data = get_request_data(mass_request.id)

    mass_request = update_mass_request(mass_request, status=IN_PROGRESS)

    items = request_data.get('items')
    options = {**request_data, "items": None}

    if not items:
        update_mass_request(
            mass_request, status=FAILED, error="items not found in the request")
        return

    logger.info(f"Total items : {mass_request.total}")
    chunked_items = get_chunked_items(items, OUTER_CHUNKS)
    total_chunks = len(chunked_items)

    data = []
    for i, items_chunk in enumerate(chunked_items):
        logger.info(f"Processing chunk : {i+1}/{total_chunks}")

        results = make_items_chunk_requests(items_chunk, options)

        data += get_items_from_results(results)
        mass_request = update_mass_request(mass_request, completed=len(data))

        logger.info(f"Completed : {
                    mass_request.completed}/{mass_request.total}")

    upload_data(mass_request.id, data)
    update_mass_request(mass_request, status=COMPLETED, completion_timestamp=datetime.now())

    notify_callback(mass_request.id)
    update_mass_request(mass_request, is_synced=True)


main()
