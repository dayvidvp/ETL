import time
from loguru import logger


@logger.catch
def timeis(func):
    '''Decorator that reports the execution time.'''

    def wrap(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        logger.info(f"{func.__name__}: Took {(end - start).__round__(4)} seconds")
        return result
    return wrap
