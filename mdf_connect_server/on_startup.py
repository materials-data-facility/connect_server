# This script is to be run by the start_connect.sh script.
# start_connect.sh will only run this script on API startup, one time when tha API is started.

import logging

from mdf_connect_server import CONFIG, utils


# Identical setup to API
# Set up root logger
logger = logging.getLogger("mdf_connect_server")
logger.setLevel(CONFIG["LOG_LEVEL"])
logger.propagate = False
# Set up formatters
logfile_formatter = logging.Formatter("{message}", style='{')
# Set up handlers
logfile_handler = logging.FileHandler(CONFIG["API_LOG_FILE"], mode='a')
logfile_handler.setFormatter(logfile_formatter)

logger.addHandler(logfile_handler)

logger.info("\n\n==========Initiating Connect API startup tasks==========\n")

logger.info("Deleting old test submissions")
utils.purge_old_tests(dry_run=False)

logger.info("\n\n==========Connect API startup tasks complete==========\n")
