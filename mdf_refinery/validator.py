import os
import json

import jsonschema

DATASET_SCHEMA = os.path.join(os.path.dirname(__file__), "dataset.schema")
RECORD_SCHEMA = os.path.join(os.path.dirname(__file__), "record.schema")

    
def validate_dataset(ds_md):
    """Validate a dataset"""
    # Add validator fields
    ds_md["metadata_version"] = "0.5.0"

    # Validate against schema
    with open(DATASET_SCHEMA) as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(ds_md, schema)
    except jsonschema.ValidationError as e:
        return {
            "success": False,
            "error": "Invalid metadata: " + str(e).split("\n")[0],
            "details": str(e)
            }
    # Return results
    return {
        "success": True,
        "valid": ds_md
        }


def validate_record(rc_md):
    """Validate a record."""
    # Add validator fields
    rc_md["metadata_version"] = "0.5.0"

    # Validate against schema
    with open(RECORD_SCHEMA) as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(rc_md, schema)
    except jsonschema.ValidationError as e:
        return {
            "success": False,
            "error": "Invalid metadata: " + str(e).split("\n")[0],
            "details": str(e)
            }
    # Return results
    return {
        "success": True,
        "valid": rc_md
        }    

