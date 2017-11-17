from datetime import datetime
import json
import os
from tempfile import TemporaryFile

from bson import ObjectId
import jsonschema


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
    def __init__(schema_path=None, mdf_finalize=False):
        self.__dataset = None  # Serves as initialized flag
        self.__tempfile = None
        self.__scroll_id = None
        self.__ingest_date = datetime.utcnow().isoformat("T") + "Z"
        self.__finished = None  # Flag - has user called get_finished_dataset() for this dataset?
        self.__finalize = mdf_finalize
        if schema_path:
            self.__schema_dir = schema_path
        else:
            self.__schema_dir = os.path.join(os.path.dirname(__file__), "schemas")


    def __make_source_name(title):
        """Make a source_name out of a title."""
        stopwords = [
            "and",
            "or",
            "the",
            "a",
            "an",
            "of"
        ]
        title = title.strip().lower()
        # Replace words we don't want
        for stopword in stopwords:
            title = title.replace(stopword, "")
        # Clear double spacing
        while title.find("  ") != -1:
            title = title.replace("  ", " ")
        # Replace spaces with underscores
        title = title.replace(" ", "_")
        # Replace characters we don't want/can't use
        if not title.isalnum():
            source_name = ""
            [source_name += char for char in title if char.isalnum()]
        else:
            source_name = title

        return source_name


    def start_dataset(ds_md):
        """Validate a dataset against the MDF schema.

        Arguments:
        ds_md (dict): The dataset metadata to validate.

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

        # Load schema
        with open(os.path.join(SCHEMA_DIR, "dataset_schema.json")) as schema_file:
            schema = json.load(schema_file)
        # Load MDF block
        with open(os.path.join(SCHEMA_DIR, "mdf_schema.json")) as mdf_file:
            schema["mdf"] = json.load(mdf_file)

        # Add any missing blocks
        if not ds_md.get("dc"):
            ds_md["dc"] = {}
        if not ds_md.get("mdf"):
            ds_md["mdf"] = {}
        if not ds_md.get("file_bags"):
            ds_md["file_bags"] = {}
        if not ds_md.get("publications"):
            ds_md["publications"] = []
        if not ds_md.get("mrr"):
            ds_md["mrr"] = {}

        # Add fields
        if self.__inalize:
            # Finalization fields are computed
            #TODO: dc?

            # BLOCK: mdf
            # mdf_id
            ds_md["mdf"]["mdf_id"] = str(ObjectId())

            # scroll_id
            self.__scroll_id = 0
            ds_md["mdf"]["scroll_id"] = self.__scroll_id
            self.__scroll_id += 1

            # parent_id
            # Not Implemented

            # ingest_date
            ds_md["mdf"]["ingest_date"] = self.__ingest_date

            # resource_type
            ds_md["mdf"]["resource_type"] = "dataset"

            # source_name
            if not ds_md["mdf"].get("source_name"):
                try:
                    ds_md_["mdf"]["source_name"] = make_source_name(
                                                    ds_md["dc"]["titles"][0]["title"])
                except (KeyError, ValueError):
                    # DC title is required, ds_md will fail validation
                    # Doesn't really matter what this is
                    ds_md["mdf"["source_name"] = "unknown"

            # acl
            if not ds_md["mdf"].get("acl"):
                ds_md["mdf"]["acl"] = ["public"]

            # version
            if not ds_md["mdf"].get("version"):
                ds_md["mdf"]["version"] = 1

        else:
            # Add placeholder data instead


        # Validate against schema
        try:
            jsonschema.validate(ds_md, schema)
        except jsonschema.ValidationError as e:
            return {
                "success": False,
                "error": "Invalid metadata: " + str(e).split("\n")[0],
                "details": str(e)
                }

        # Create temporary file for records
        self.__tempfile = TemporaryFile()

        # Save dataset metadata
        self.__dataset = json.loads(json.dumps(ds_md))

        # Return results
        return {
            "success": True
            }


    def add_record(rc_md):
        """Validate a record against the MDF schema.

        Arguments:
        rc_md (dict): The record metadata to validate.

        Returns:
        dict: success (bool): True on success, False on failure
            If success is False:
              error (str): A short message about the error.
              details (str): The full jsonschema error message.
        """
        if not self.__dataset:
            return {
                "success": False,
                "error": "Dataset not started."
                }
        elif finished:
            return {
                "success": False,
                "error": ("Dataset has been finished by calling get_finished_dataset(),"
                          " and no more records may be entered.")
                }

        # Load schema
        with open(os.path.join(SCHEMA_DIR, "record_schema.json")) as schema_file:
            schema = json.load(schema_file)
        # Load MDF block
        with open(os.path.join(SCHEMA_DIR, "mdf_schema.json")) as mdf_file:
            schema["mdf"] = json.load(mdf_file)

        # Add fields
        if finalize:
            # Finalization fields are computed
            # mdf
            # source_name
            rc_md["mdf"]["source_name"] = self.__dataset["mdf"]["source_name"]
        else:
            # Add placeholder data instead


        # Validate against schema
        try:
            jsonschema.validate(rc_md, schema)
        except jsonschema.ValidationError as e:
            return {
                "success": False,
                "error": "Invalid metadata: " + str(e).split("\n")[0],
                "details": str(e)
                }

        # Write out to file
        json.dump(rc_md, self.__tempfile)
        self.__tempfile.write("\n")

        # Return results
        return {
            "success": True
            }


    def get_finished_dataset():
        """Retrieve finished dataset, in a generator."""
        if not self.__dataset:
            raise ValueError("Dataset not started")
        elif self.__finished:
            raise ValueError("Dataset already finished")

        #TODO: Add data into dataset entry
        self.__finished = True

        self.__tempfile.seek(0)

        yield self.__dataset
        for line in self.__tempfile:
            yield json.loads(line)

        self.__tempfile.close()
        self.__dataset = None
        return


