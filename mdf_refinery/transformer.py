import json

import ase.io
from mdf_toolbox import toolbox
import pandas as pd
from PIL import Image
import pymatgen
from pymatgen.io.ase import AseAtomsAdaptor as ase_to_pmg
from pif_ingestor.manager import IngesterManager
from pypif.pif import dump as pif_dump
from pypif_sdk.util import citrination as cit_utils
from pypif_sdk.interop.mdf import _to_user_defined as pif_to_feedstock
from pypif_sdk.interop.datacite import add_datacite as add_dc
from queue import Empty


# data_format to data_type translations
FORMAT_TYPE = {
    "vasp": "dft"
}

# Additional NaN values for Pandas
NA_VALUES = ["", " "]

# List of parsers at bottom
# All parsers accept data_path and/or file_data, and arbitrary other parameters


def transform(input_queue, output_queue, queue_done, parse_params):
    """Parse data files however possible.

    Arguments:
    group (list of str): One group of files to parse.
    parse_params (dict): Run parsers with these parameters.
        dataset (dict): The dataset entry.
        parsers (dict): The parser-specific information.

    Returns:
    list of dict: The metadata parsed from the file.
                  Will be empty if no selected parser can parse data.
    """
    try:
        # Parse each group from the queue
        # Exit loop when queue_done is True and no groups remain
        while True:
            # Fetch group from queue
            try:
                group = input_queue.get(timeout=5)
            # No group fetched
            except Empty:
                # Queue is permanently depleted, stop processing
                if queue_done.value:
                    break
                # Queue is still active, try again
                else:
                    continue

            print("DEBUG: Fetched group", group)
            # Process fetched group
            single_record = {}
            multi_records = []
            for parser in ALL_PARSERS:
                # TODO: Filter appropriate parsers
                if True or parser.__name__ in parse_params.get("parsers", {}).keys():
                    try:
                        parser_res = parser(group=group, params=parse_params)
                    except Exception as e:
                        print("Parser {p} failed with exception {e}".format(
                                                                        p=parser.__name__,
                                                                        e=repr(e)))
                    else:
                        # Only process actual results
                        if parser_res:
                            # If a single record was returned, merge with others
                            if isinstance(parser_res, dict):
                                single_record = toolbox.dict_merge(single_record, parser_res)
                            # If multiple records were returned, add to list
                            elif isinstance(parser_res, list):
                                # Only add records with data
                                [multi_records.append(rec) for rec in parser_res if rec]
                            # Else, panic
                            else:
                                raise TypeError(("Parser '{p}' returned "
                                                 "type '{t}'!").format(p=parser.__name__,
                                                                       t=type(parser_res)))
                        else:
                            print("DEBUG:", parser.__name__, "unable to parse")
            # Merge the single_record into all multi_records if both exist
            if single_record and multi_records:
                records = [toolbox.dict_merge(r, single_record) for r in multi_records if r]
            # Else, if single_record exists, make it a list
            elif single_record:
                records = [single_record]
            # Otherwise, use the list of records if it exists
            elif multi_records:
                records = multi_records
            # If nothing exists, make a blank list
            else:
                records = []

            # Push records to output queue
            for record in records:
                output_queue.put(json.dumps(record))
    except Exception as e:
        print("DEBUG: Transformer error:", repr(e))


def parse_crystal_structure(group, params=None):
    """Parser for the crystal_structure block.
    Will also populate materials block.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict): N/A

    Returns:
    dict: The record parsed.
    """
    record = {}

    for data_file in group:
        materials = {}
        crystal_structure = {}
        # Attempt to read the file
        try:
            # Read with ASE
            ase_res = ase.io.read(data_file)
            # Check data read, validate crystal structure
            if not ase_res or not all(ase_res.get_pbc()):
                raise ValueError("No valid data")
            else:
                # Convert ASE Atoms to Pymatgen Structure
                pmg_s = ase_to_pmg.get_structure(ase_res)
        # ASE failed to read file
        except Exception as e:
            try:
                # Read with Pymatgen
                pmg_s = pymatgen.Structure.from_file(data_file)
            except Exception:
                # Can't read file
                continue

        # Parse materials block
        materials["composition"] = pmg_s.formula.replace(" ", "")
        # Parse crystal_structure block
        crystal_structure["space_group_number"] = pmg_s.get_space_group_info()[1]
        crystal_structure["number_of_atoms"] = int(pmg_s.composition.num_atoms)
        crystal_structure["volume"] = float(pmg_s.volume)

        # Add to record
        record = toolbox.dict_merge(record, {
                                                "materials": materials,
                                                "crystal_structure": crystal_structure
                                            })
    return record


# TODO
def parse_pif(data_path, params=None):
    """Use Citrine's parsers."""
    # TODO: Should this be elsewhere?
    cit_manager = IngesterManager()

    cit_pifs = cit_manager.run_extensions(group_paths, include=None, exclude=[],
                                          args={"quality_report": False})
    cit_pifs = cit_utils.set_uids(cit_pifs)


def parse_csv(group, params=None):
    """Parser for CSVs.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        parsers (dict):
            csv (dict):
                mapping (dict): The mapping of mdf_fields: csv_headers
                delimiter (str): The delimiter. Default ','
                na_values (list of str): Values to treat as N/A. Default NA_VALUES

    Returns:
    dict: The record(s) parsed.
    """
    try:
        csv_params = params["parsers"]["csv"]
        mapping = csv_params["mapping"]
    except KeyError:
        return {}

    for file_path in group:
        df = pd.read_csv(file_path, delimiter=csv_params.get("delimiter", ","), na_values=NA_VALUES)
    # TODO
    return parse_pandas(df, params.get("mapping", {}))


# TODO
def parse_excel(file_data=None, params=None, **ignored):
    """Parse an Excel file."""
    if not params or not file_data:
        return {}
    df = pd.read_excel(file_data, na_values=NA_VALUES)
    return parse_pandas(df, params.get("mapping", {}))


# TODO
def parse_json(file_data=None, params=None, **ignored):
    """Parse a JSON file."""
    # If no structure is supplied, do no parsing
    if not params or not file_data:
        return {}
    records = []
    if not isinstance(file_data, dict) or isinstance(file_data, list):
        file_json = json.load(file_data)
    else:
        file_json = file_data
    try:
        mapping = params.pop("mapping")
    except KeyError:
        mapping = params

    # Handle lists of JSON documents as separate records
    if not isinstance(file_json, list):
        file_json = [file_json]

    for data in file_json:
        record = {}
        # Get (path, value) pairs from the key structure
        # Loop over each
        for mdf_path, json_path in flatten_struct(mapping):
            try:
                value = follow_path(data, json_path)
            except KeyError:
                value = None
            # Only add value if value exists
            if value is not None:
                fields = mdf_path.split(".")
                last_field = fields.pop()
                current_field = record
                # Create all missing fields
                for field in fields:
                    if current_field.get(field) is None:
                        current_field[field] = {}
                    current_field = current_field[field]
                # Add value to end
                current_field[last_field] = value
        # Add record to list if exists
        if record:
            records.append(record)

    return records


# TODO
def parse_image(data_path=None, **ignored):
    """Parse an image."""
    im = Image.open(data_path)
    return {
        "image": {
            "width": im.width,
            "height": im.height,
            "pixels": im.width * im.height,
            "format": im.format
        }
    }


# List of all user-selectable parsers
# TODO: Repopulate
ALL_PARSERS = [
    parse_crystal_structure
]


# TODO
def parse_pandas(df, mapping):
    """Parse a Pandas DataFrame."""
    csv_len = len(df.index)
    df_json = json.loads(df.to_json())

    records = []
    for index in range(csv_len):
        new_map = {}
        for path, value in flatten_struct(mapping):
            new_map[path] = value + "." + str(index)
        rec = parse_json(df_json, new_map)
        if rec:
            records.append(rec)
    return records


def flatten_struct(struct, path=""):
    """Take a dict structure and flatten into dot notation.
    Path will be prepended if supplied.

    ex. {
            key1: {
                key2: value
            }
        }
        turns into
        (key1.key2, value)
    Tuples are yielded.
    """
    for key, val in struct.items():
        if isinstance(val, dict):
            for p in flatten_struct(val, path+"."+key):
                yield p
        else:
            yield ((path+"."+key).strip(". "), val)


def follow_path(json_data, json_path):
    """Get the value in the data pointed to by the path."""
    value = json_data
    for field in json_path.split("."):
        value = value[field]
    return value
