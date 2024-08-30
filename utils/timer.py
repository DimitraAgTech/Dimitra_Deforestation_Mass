import time
from functools import wraps

from utils.logger import logger

def time_it(func):
    """This decorator prints the execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        time_taken = end - start
        logger.info(f"{func.__name__}: {time_taken:.4f} seconds")
        return result

    return wrapper
