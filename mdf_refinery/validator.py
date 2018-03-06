from datetime import datetime
import json
import os
from tempfile import TemporaryFile

from bson import ObjectId
from crossref.restful import Works as Crossref
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
    def __init__(self, schema_path=None):
        self.__dataset = None  # Serves as initialized flag
        self.__tempfile = None
        self.__scroll_id = None
        self.__ingest_date = datetime.utcnow().isoformat("T") + "Z"
        self.__indexed_files = []
        self.__finished = None  # Flag - has user called get_finished_dataset() for this dataset?
        if schema_path:
            self.__schema_dir = schema_path
        else:
            self.__schema_dir = os.path.join(os.path.dirname(__file__), "schemas")

    def __make_source_name(self, title):
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
            title = title.replace(" " + stopword + " ", " ")
        # Clear double spacing
        while title.find("  ") != -1:
            title = title.replace("  ", " ")
        # Replace spaces with underscores
        title = title.replace(" ", "_")
        # Replace characters we don't want/can't use
        if not title.isalnum():
            source_name = ""
            for char in title:
                if char.isalnum() or char == " ":
                    source_name += char
        else:
            source_name = title

        return source_name

    def start_dataset(self, ds_md):
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
        with open(os.path.join(self.__schema_dir, "dataset.json")) as schema_file:
            schema = json.load(schema_file)
        # Load MDF block
        with open(os.path.join(self.__schema_dir, "mdf.json")) as mdf_file:
            schema["properties"]["mdf"] = json.load(mdf_file)

#        if not ds_md.get("dc") or not isinstance(ds_md["dc"], dict):
#            ds_md["dc"] = {}
        if not ds_md.get("mdf") or not isinstance(ds_md["mdf"], dict):
            ds_md["mdf"] = {}
        if not ds_md.get("file_bags") or not isinstance(ds_md["file_bags"], dict):
            ds_md["file_bags"] = {}
#        if not ds_md.get("publications") or not isinstance(ds_md["publications"], list):
#            ds_md["publications"] = []
#        if not ds_md.get("mrr") or not isinstance(ds_md["mrr"], dict):
#            ds_md["mrr"] = {}

        # Add fields
        # TODO: dc?

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

        # TODO: Remove?
        # source_name
        # if not ds_md["mdf"].get("source_name"):
        #    try:
        #        ds_md["mdf"]["source_name"] = self.__make_source_name(
        #                                        ds_md["dc"]["titles"][0]["title"])
        #    except (KeyError, ValueError, IndexError):
        #        # DC title is required, ds_md will fail validation
        #        # Doesn't really matter what this is
        #        ds_md["mdf"]["source_name"] = "unknown"

        # acl
        if not ds_md["mdf"].get("acl"):
            ds_md["mdf"]["acl"] = ["public"]

        # version
        if not ds_md["mdf"].get("version"):
            ds_md["mdf"]["version"] = 1

        # BLOCK: file_bags
        # None?

        # BLOCK: publications
        new_pubs = []
        cref = Crossref()
        for doi in ds_md.get("publications", []):
            # If doi refers to a DOI
            if isinstance(doi, str):
                pub_md = cref.doi(doi)
                # doi call will return None if not found
                if isinstance(pub_md, dict):
                    new_pubs.append(pub_md)
                # Maintain DOI if not found
                else:
                    new_pubs.append({"doi": doi})
            # If is dict, assume is metadata
            elif isinstance(doi, dict):
                new_pubs.append(doi)
            # Else, is not appropriate data and is discarded

        if new_pubs:
            ds_md["publications"] = new_pubs

        # BLOCK: mrr
        # None?

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
        if not self.__dataset:
            return {
                "success": False,
                "error": "Dataset not started."
                }
        elif self.__finished:
            return {
                "success": False,
                "error": ("Dataset has been finished by calling get_finished_dataset(),"
                          " and no more records may be entered.")
                }

        # Load schema
        with open(os.path.join(self.__schema_dir, "record.json")) as schema_file:
            schema = json.load(schema_file)
        # Load MDF block
        with open(os.path.join(self.__schema_dir, "mdf.json")) as mdf_file:
            schema["properties"]["mdf"] = json.load(mdf_file)

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
        # source_name
        rc_md["mdf"]["source_name"] = self.__dataset["mdf"]["source_name"]

        # mdf_id
        rc_md["mdf"]["mdf_id"] = str(ObjectId())

        # scroll_id
        rc_md["mdf"]["scroll_id"] = self.__scroll_id
        self.__scroll_id += 1

        # parent_id
        rc_md["mdf"]["parent_id"] = self.__dataset["mdf"]["mdf_id"]

        # ingest_date
        rc_md["mdf"]["ingest_date"] = self.__ingest_date

        # resource_type
        rc_md["mdf"]["resource_type"] = "record"

        # acl
        if not rc_md["mdf"].get("acl"):
            rc_md["mdf"]["acl"] = self.__dataset["mdf"]["acl"]

        # landing_page
        if not rc_md["mdf"].get("landing_page"):
            rc_md["mdf"]["landing_page"] = (self.__dataset["mdf"]["landing_page"]
                                            + "#"
                                            + str(rc_md["mdf"]["scroll_id"]))

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
            # Currently deprecated
            # If any "element" isn't in the periodic table,
            # the composition is likely not a chemical formula and should not be parsed
#                if all([elem in DICT_OF_ALL_ELEMENTS.values() for elem in list_of_elem]):
#                    record["elements"] = list_of_elem

            rc_md["material"]["elements"] = list_of_elem

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

    def get_finished_dataset(self):
        """Retrieve finished dataset, in a generator."""
        if not self.__dataset:
            raise ValueError("Dataset not started")
        elif self.__finished:
            raise ValueError("Dataset already finished")

        # Add data into dataset entry
        # TODO: Make bags, mint minid

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
