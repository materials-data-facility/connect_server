import os
import json

import jsonschema

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")


def validate_dataset(ds_md, finalize=False):
    """Validate a dataset against the MDF schema.

    Arguments:
    ds_md (dict): The dataset metadata to validate.
    finalize (bool): Is this the finalizing validation before ingestion?
                     For all purposes except some MDF internal services, thish should be False.

    Returns:
    dict: success (bool): True on success, False on failure
        If success is True:
          valid (dict): The validated and processed metadata.
        If success is False:
          error (str): A short message about the error.
          details (str): The full jsonschema error message.
    """
    # Load schema
    with open(os.path.join(SCHEMA_DIR, "basic_dataset.schema")) as base_schema_file:
        schema = json.load(base_schema_file)
    # If finalizing, load final additions to schema
    if finalize:
        with open(os.path.join(SCHEMA_DIR, "final_dataset.schema")) as final_schema_file:
            schema = deep_merge(schema, json.load(final_schema_file))

    # Add validator fields
    ds_md["metadata_version"] = "0.5.0"

    # Add finalizer fields
    if finalize:
        ds_md["finalized"] = True

    # Validate against schema
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


def validate_record(rc_md, finalize=False):
    """Validate a record against the MDF schema.

    Arguments:
    rc_md (dict): The record metadata to validate.
    finalize (bool): Is this the finalizing validation before ingestion?
                     For all purposes except some MDF internal services, this should be False.

    Returns:
    dict: success (bool): True on success, False on failure
        If success is True:
          valid (dict): The validated and processed metadata.
        If success is False:
          error (str): A short message about the error.
          details (str): The full jsonschema error message.
    """
    # Load schema
    with open(os.path.join(SCHEMA_DIR, "basic_record.schema")) as base_schema_file:
        schema = json.load(base_schema_file)
    # If finalizing, load final additions to schema
    if finalize:
        with open(os.path.join(SCHEMA_DIR, "final_record.schema")) as final_schema_file:
            schema = deep_merge(schema, json.load(final_schema_file))

    # Add validator fields
    rc_md["metadata_version"] = "0.5.0"

    # Add finalizer fields
    if finalize:
        rc_md["finalized"] = True

    # Validate against schema
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


# Utility function
# Will merge into Toolbox if useful elsewhere
def deep_merge(base, update):
    """Deeply update a base dictionary with values from an update dictionary.

    Arguments:
    base (dict): The default dictionary.
    update (dict): The overwriting dictionary.

    Returns:
    dict: The base dictionary with the values in the update dictionary added.
    """
    for key, value in update.items():
        if isinstance(value, collections.Mapping):
            base[key] = update(base.get(key, {}), value)
        else:
            base[key] = value
    return base

