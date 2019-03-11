import json
import os

import jsonschema

from mdf_connect_server import CONFIG


def test_organizations():
    # Test all org entries to ensure they match schema
    # Load schema
    with open(os.path.join(CONFIG["SCHEMA_PATH"], "organization.json")) as f:
        schema = json.load(f)
    # Test each org
    for org in CONFIG["ORGANIZATIONS"]:
        # Require JSON
        json.dumps(org, allow_nan=False)
        # Validate against schema
        jsonschema.validate(org, schema)
