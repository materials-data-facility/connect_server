from flask import Flask

app = Flask(__name__)
app.config.from_pyfile("api.conf")

import moc.api
