import traceback

from config import BATCH_SIZE, TASK_TYPE, WORKERS
from constants import FAILED
from task.task import run_mass_deforestation_request
from utils.logger import logger
from utils.request import get_available_mass_request, update_mass_request


def main():
    mass_request = get_available_mass_request()
    if not mass_request:
        logger.info("No PENDING requests found, quitting the service")
        return

    logger.info(f"Processing request_id : {
                mass_request.id} with status : {mass_request.status}")
    logger.info(f"Task type : {TASK_TYPE}")
    logger.info(f"Using batch size : {BATCH_SIZE}")
    logger.info(f"Using workers : {WORKERS}")

    try:
        run_mass_deforestation_request(mass_request)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(e)
        update_mass_request(mass_request, is_synced=False,
                            status=FAILED, error=str(e))


main()
