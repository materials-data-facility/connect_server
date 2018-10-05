import os

from mdf_toolbox import dict_merge

from mdf_connect_server.config import DEFAULT, DEV, KEYS, PROD


CONFIG = {}
CONFIG = dict_merge(DEFAULT, CONFIG)
CONFIG = dict_merge(KEYS, CONFIG)

server = os.environ.get("FLASK_ENV")
if server == "production":
    CONFIG = dict_merge(PROD, CONFIG)
elif server == "development":
    CONFIG = dict_merge(DEV, CONFIG)
else:
    raise EnvironmentError("FLASK_ENV not correctly set! FLASK_ENV must be 'production'"
                           " or 'development', even for processing only.")

from mdf_connect_server.utils import utils  # noqa: E402,F401
# NOTE: flake8 complains about import not at top and import unused; this is fine

# Make required dirs
os.makedirs(CONFIG["LOCAL_PATH"], exist_ok=True)
os.makedirs(CONFIG["FEEDSTOCK_PATH"], exist_ok=True)
os.makedirs(CONFIG["SERVICE_DATA"], exist_ok=True)
