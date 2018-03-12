from .transformer import transform
from .converter import convert
from .validator import Validator
from .search_ingester import search_ingest
from flask import Flask

app = Flask(__name__)
app.config.from_pyfile("api.conf")

import api
