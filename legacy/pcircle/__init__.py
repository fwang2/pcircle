

# from ._version import get_versions
# __version__ = get_versions()['version']
# del get_versions
#
# #Set default logging handler to avoid "No handler found" warnings.
# import logging
# try:  # Python 2.7+
#     from logging import NullHandler
# except ImportError:
#     class NullHandler(logging.Handler):
#         def emit(self, record):
#             pass
#
# logging.getLogger(__name__).addHandler(NullHandler())

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
