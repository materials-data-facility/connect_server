from datetime import datetime
import json
import os
from tempfile import TemporaryFile

import jsonschema


def _remove_nulls(data, skip=None):
    """Remove all null/None/empty values from a dict or list, except those listed in skip."""
    if isinstance(data, dict):
        new_dict = {}
        for key, val in data.items():
            new_val = _remove_nulls(val, skip=skip)
            if new_val is not None or (skip is not None and key in skip):
                new_dict[key] = new_val
        return new_dict
    elif isinstance(data, list):
        new_list = []
        for val in data:
            new_val = _remove_nulls(val, skip=skip)
            if new_val is not None:
                new_list.append(new_val)
        return new_list
    # Could delete required but empty blocks - services, etc.
    # elif hasattr(data, "__len__") and len(data) <= 0:
    #    return None
    else:
        return data


class Validator:
    """Validates MDF feedstock.

    Flow:
        start_dataset(dataset_metadata)
        (check if returned success)
        for record in records:
            add_record(record)
            (success check)
        gen = get_finished_dataset()
    """
    def __init__(self, schema_path):
        self.__dataset = None  # Serves as initialized flag
        self.__tempfile = None
        self.__scroll_id = None
        self.__ingest_date = datetime.utcnow().isoformat("T") + "Z"
        self.__indexed_files = []
        self.__finished = None  # Flag - has user called get_finished_dataset() for this dataset?
        self.__schema_dir = schema_path

    def start_dataset(self, ds_md, validation_info=None):
        """Validate a dataset against the MDF schema.

        Arguments:
        ds_md (dict): The dataset metadata to validate.
        validation_info (dict): Additional validation configuration.

        Returns:
        dict: success (bool): True on success, False on failure
            If success is False:
              error (str): A short message about the error.
              details (str): The full jsonschema error message.
        """
        if self.__dataset is not None:
            return {
                "success": False,
                "error": "Dataset validation already in progress."
                }
        self.__finished = False

        if validation_info is None:
            validation_info = {}
        self.__project_blocks = validation_info.get("project_blocks", None)
        self.__required_fields = validation_info.get("required_fields", None)
        self.__allowed_nulls = validation_info.get("allowed_nulls", None)
        self.__base_acl = validation_info.get("base_acl", None)

        # Load schema
        with open(os.path.join(self.__schema_dir, "dataset.json")) as schema_file:
            schema = json.load(schema_file)
        resolver = jsonschema.RefResolver(base_uri="file://{}/".format(self.__schema_dir),
                                          referrer=schema)

#        if not ds_md.get("dc") or not isinstance(ds_md["dc"], dict):
#            ds_md["dc"] = {}
        if not ds_md.get("mdf") or not isinstance(ds_md["mdf"], dict):
            ds_md["mdf"] = {}
#        if not ds_md.get("mrr") or not isinstance(ds_md["mrr"], dict):
#            ds_md["mrr"] = {}

        # Add fields
        # BLOCK: dc
        # TODO

        # BLOCK: mdf
        # scroll_id
        self.__scroll_id = 0
        ds_md["mdf"]["scroll_id"] = self.__scroll_id
        self.__scroll_id += 1

        # ingest_date
        ds_md["mdf"]["ingest_date"] = self.__ingest_date

        # resource_type
        ds_md["mdf"]["resource_type"] = "dataset"

        # mdf-block fields source_id and source_name must already be set
        # (should be the case in correct preprocessing of submission)

        # acl
        if not ds_md["mdf"].get("acl"):
            ds_md["mdf"]["acl"] = self.__base_acl or ["public"]

        # version
        if not ds_md["mdf"].get("version"):
            ds_md["mdf"]["version"] = 1

        # BLOCK: mrr
        # TODO

        # Services
        ds_md["services"] = ds_md.get("services", {})

        # Data
        ds_md["data"] = ds_md.get("data", {})

        # BLOCK: custom
        # Make all values into strings
        if ds_md.get("custom"):
            new_custom = {}
            for key, val in ds_md["custom"].items():
                new_custom[key] = str(val)
            ds_md["custom"] = new_custom

        # Require strict JSON
        try:
            json.dumps(ds_md, allow_nan=False)
        except (ValueError, json.JSONDecodeError) as e:
            return {
                "success": False,
                "error": "Invalid dataset JSON: {}".format(str(e)),
                "details": repr(e)
            }

        # Remove null/None values
        ds_md = _remove_nulls(ds_md, self.__allowed_nulls)

        # Validate against schema
        try:
            jsonschema.validate(ds_md, schema, resolver=resolver)
        except jsonschema.ValidationError as e:
            return {
                "success": False,
                "error": "Invalid dataset metadata: " + str(e).split("\n")[0],
                "details": str(e)
            }

        # Check projects blocks allowed
        # If no blocks, disallow projects
        if not self.__project_blocks:
            if ds_md.get("projects"):
                return {
                    "success": False,
                    "error": "Unauthorized project metadata: No projects allowed",
                    "details": "'project' block not allowed: '{}'".format(ds_md)
                }
        # If some project blocks allowed, check that only allowed ones are present
        else:
            unauthorized = []
            for proj in ds_md.get("projects", {}).keys():
                if proj not in self.__project_blocks:
                    unauthorized.append(proj)
            if unauthorized:
                return {
                    "success": False,
                    "error": ("Unauthorized project metadata: '{}' not allowed"
                              .format(unauthorized)),
                    "details": ("Not authorized for project block(s) '{}' in '{}'. "
                                "The dataset is not in an allowed organization."
                                .format(unauthorized, ds_md))
                }

        # Validate required fields
        # TODO: How should this validation be done?
        # The metadata conforms to the schema, there are just extra
        # `requires` values. Perhaps add these to the schema instead?
        # Lists, specifically, are an issue. Must all dicts in the list
        # conform? This behavior is difficult.
        # As a semi-temporary measure, only check the first element of lists.
        if self.__required_fields:
            missing = []
            for field_path in self.__required_fields:
                value = ds_md
                for field_name in field_path.split("."):
                    try:
                        value = value[field_name]
                        if isinstance(value, list) and len(value) > 0:
                            value = value[0]
                    except KeyError:
                        missing.append(field_path)
                        break
            if missing:
                return {
                    "success": False,
                    "error": "Missing organization metadata: '{}' are required".format(missing),
                    "details": ("Required fields are '{}', but '{}' are missing"
                                .format(self.__required_fields, missing))
                }

        # Create temporary file for records
        self.__tempfile = TemporaryFile(mode="w+")

        # Save dataset metadata
        # Also ensure metadata is JSON-serializable
        self.__dataset = json.loads(json.dumps(ds_md))

        # Return results
        return {
            "success": True
            }

    def add_record(self, rc_md):
        """Validate a record against the MDF schema.

        Arguments:
        rc_md (dict): The record metadata to validate.

        Returns:
        dict: success (bool): True on success, False on failure
            If success is False:
              error (str): A short message about the error.
              details (str): The full jsonschema error message.
        """
        if self.__finished:
            return {
                "success": False,
                "error": ("Dataset has been finished by calling get_finished_dataset(),"
                          " and no more records may be entered.")
                }
        elif not self.__dataset:
            return {
                "success": False,
                "error": "Dataset not started."
                }

        # Load schema
        with open(os.path.join(self.__schema_dir, "record.json")) as schema_file:
            schema = json.load(schema_file)
        resolver = jsonschema.RefResolver(base_uri="file://{}/".format(self.__schema_dir),
                                          referrer=schema)

        # Add any missing blocks
        if not rc_md.get("mdf"):
            rc_md["mdf"] = {}
        if not rc_md.get("files"):
            rc_md["files"] = []
        elif isinstance(rc_md["files"], dict):
            rc_md["files"] = [rc_md["files"]]
        if not rc_md.get("material"):
            rc_md["material"] = {}

        # Add fields
        # BLOCK: mdf
        # source_id
        rc_md["mdf"]["source_id"] = self.__dataset["mdf"]["source_id"]

        # source_name
        rc_md["mdf"]["source_name"] = self.__dataset["mdf"]["source_name"]

        # scroll_id
        rc_md["mdf"]["scroll_id"] = self.__scroll_id
        self.__scroll_id += 1

        # ingest_date
        rc_md["mdf"]["ingest_date"] = self.__ingest_date

        # resource_type
        rc_md["mdf"]["resource_type"] = "record"

        # version
        rc_md["mdf"]["version"] = self.__dataset["mdf"]["version"]

        # acl
        if not rc_md["mdf"].get("acl"):
            rc_md["mdf"]["acl"] = self.__base_acl or self.__dataset["mdf"]["acl"]

        # organizations
        if self.__dataset["mdf"].get("organizations"):
            rc_md["mdf"]["organizations"] = self.__dataset["mdf"]["organizations"]

        # BLOCK: files
        # Add file data to dataset
        if rc_md["files"]:
            self.__indexed_files += rc_md["files"]

        # BLOCK: material
        # elements
        if rc_md["material"].get("composition"):
            composition = rc_md["material"]["composition"].replace("and", "")
            # Currently deprecated
#                for element in DICT_OF_ALL_ELEMENTS.keys():
#                    composition = re.sub("(?i)"+element,
#                                         DICT_OF_ALL_ELEMENTS[element], composition)
            str_of_elem = ""
            for char in list(composition):
                if char.isupper():  # Start of new element symbol
                    str_of_elem += " " + char
                elif char.islower():  # Continuation of symbol
                    str_of_elem += char
                # Anything else is not an element (numbers, whitespace, etc.)

            # Split elements in string (on whitespace), make unique and JSON-serializable
            list_of_elem = list(set(str_of_elem.split()))
            # Ensure deterministic results
            list_of_elem.sort()
            # Currently deprecated
            # If any "element" isn't in the periodic table,
            # the composition is likely not a chemical formula and should not be parsed
#                if all([elem in DICT_OF_ALL_ELEMENTS.values() for elem in list_of_elem]):
#                    record["elements"] = list_of_elem

            rc_md["material"]["elements"] = list_of_elem
        elif rc_md["material"].get("elemental_proportions"):
            rc_md["material"]["elements"] = [rc_md["material"]["elemental_proportions"].keys()]
            rc_md["material"]["elements"].sort()

        # BLOCK: custom
        # Make all values into strings
        if rc_md.get("custom"):
            new_custom = {}
            for key, val in rc_md["custom"].items():
                new_custom[key] = str(val)
            rc_md["custom"] = new_custom

        # Require strict JSON
        try:
            json.dumps(rc_md, allow_nan=False)
        except (ValueError, json.JSONDecodeError) as e:
            return {
                "success": False,
                "error": "Invalid record JSON: {}".format(str(e)),
                "details": repr(e)
                }

        # Remove null/None values
        rc_md = _remove_nulls(rc_md, self.__allowed_nulls)

        # Validate against schema
        try:
            jsonschema.validate(rc_md, schema, resolver=resolver)
        except jsonschema.ValidationError as e:
            return {
                "success": False,
                "error": "Invalid record metadata: " + str(e).split("\n")[0],
                "details": str(e)
                }

        # Write out to file
        json.dump(rc_md, self.__tempfile)
        self.__tempfile.write("\n")

        # Return results
        return {
            "success": True
            }

    def get_finished_dataset(self):
        """Retrieve finished dataset, in a generator."""
        if self.__dataset is None:
            raise ValueError("Dataset not started")
        elif self.__finished:
            raise ValueError("Dataset already finished")

        self.__indexed_files = []
        self.__finished = True

        self.__tempfile.seek(0)
        yield self.__dataset
        for line in self.__tempfile:
            yield json.loads(line)

        self.__tempfile.close()
        self.__dataset = None
        return

    def status(self):
        if self.__finished:
            if self.__dataset:
                return "Dataset finished but not fully read out."
            else:
                return "Dataset fully read out."
        else:
            if self.__dataset:
                return "Dataset started and still accepting records."
            else:
                return "Dataset not started."
