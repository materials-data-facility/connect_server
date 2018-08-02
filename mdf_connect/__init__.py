import os

from mdf_toolbox import dict_merge

from mdf_connect.config import DEFAULT, DEV, KEYS, PROD


CONFIG = {}
CONFIG = dict_merge(DEFAULT, CONFIG)
CONFIG = dict_merge(KEYS, CONFIG)

server = os.environ.get("FLASK_ENV")
if server == "production":
    #TODO: Turn on prod config for prod
    CONFIG = dict_merge(DEV, CONFIG)
    #CONFIG = dict_merge(PROD, CONFIG)
elif server == "development":
    CONFIG = dict_merge(DEV, CONFIG)
else:
    raise EnvironmentError("FLASK_ENV not correctly set! FLASK_ENV must be 'production'"
                           " or 'development', even for processing only.")
