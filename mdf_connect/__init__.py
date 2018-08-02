import os
from .transformer import transform
from .converter import convert
from .validator import Validator
from .search_ingester import search_ingest, update_search_entry
from flask import Flask


app = Flask(__name__)
# Need to pull from default.conf, keys.conf, and either prod.conf or dev.conf
app.config.from_pyfile("config/default.conf")
app.config.from_pyfile("config/keys.conf")

server = os.environ.get("FLASK_ENV")
if server == "production":
    app.config.from_pyfile("config/prod.conf")
elif server == "development":
    app.config.from_pyfile("config/dev.conf")
else:
    raise EnvironmentError("FLASK_ENV not set")

app.url_map.strict_slashes = False

from .utils import (authenticate_token, make_source_id, download_and_backup,
                    globus_publish_data, citrine_upload, cancel_submission,
                    complete_submission, validate_status, read_status, create_status,
                    update_status, modify_status_entry, translate_status)
from mdf_connect import api
