import logging


logger = logging.getLogger(__name__)


def decorator(func):
    def wrapped(*args, **keyargs):
        result = func(*args, **keyargs)
        print(result)
        return result
    return wrapped
