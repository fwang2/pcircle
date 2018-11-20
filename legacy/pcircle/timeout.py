__author__ = 'f7b'

"""
SO thread:
http://stackoverflow.com/questions/2281850/timeout-function-if-it-takes-too-long-to-finish
"""

from functools import wraps
import errno
import os
import signal


class TimeoutError(Exception):
    pass


def timeout2(seconds=10, error_message=os.strerror(errno.ETIME)):
    """
    Usage:
        from timeout import timeout
        @timeout
        def long_running_func():
            ...

        @timeout(5)
        def long_running_func2():
            ...

        # Timeout after 30 seconds, with the error "Connection timed out"
        @timeout(30, os.strerror(errno.ETIMEDOUT))
        def long_running_func3():

    """
    def decorator(func):

        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


class timeout:
    """
    Usage:
        with timeout(seconds=3):
        sleep(4)
    """

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)
