import logging
import sys

from mdf_connect_server import utils


logger = logging.getLogger("mdf_connect_server")
logger.propagate = False
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

mock_subs = [
]

utils.startup_tasks(mock_subs=None,
                    dry_run=True)

