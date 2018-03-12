from hashlib import sha512
import json
import os
from queue import Empty

# pycalphad and hyperspy imports require this env var set
os.environ["MPLBACKEND"] = "agg"

import ase.io
from bson import ObjectId
import hyperspy.api as hs
import magic
from mdf_toolbox import toolbox
import pandas as pd
from PIL import Image
import pycalphad
import pymatgen
from pymatgen.io.ase import AseAtomsAdaptor as ase_to_pmg
from pif_ingestor.manager import IngesterManager
from pypif.pif import dump as pif_dump
from pypif_sdk.util import citrination as cit_utils
from pypif_sdk.interop.mdf import _to_user_defined as pif_to_feedstock
from pypif_sdk.interop.datacite import add_datacite as add_dc
import yaml

# Additional NaN values for Pandas
NA_VALUES = ["", " "]

# List of parsers at bottom


def transform(input_queue, output_queue, queue_done, parse_params):
    """Parse data files however possible.

    Arguments:
    group (list of str): One group of files to parse.
    parse_params (dict): Run parsers with these parameters.
        dataset (dict): The dataset entry.
        parsers (dict): The parser-specific information.
        service_data (str): The path to the integration-specific data store.

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

            # Process fetched group
            single_record = {}
            multi_records = []
            for parser in ALL_PARSERS:
                try:
                    parser_res = parser(group=group, params=parse_params)
                except Exception as e:
                    print("Parser {p} failed with exception {e}".format(
                                                                    p=parser.__name__,
                                                                    e=repr(e)))
                else:
                    # If a list of one record was returned, treat as single record
                    # Eliminates [{}] from cluttering feedstock
                    # Filters one-record results from parsers that always return lists
                    if isinstance(parser_res, list) and len(parser_res) == 1:
                        parser_res = parser_res[0]
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
                        pass
                        # print("DEBUG:", parser.__name__, "unable to parse", group)
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
            # Get the file info
            try:
                file_info = _parse_file_info(group=group, params=parse_params)
            except Exception as e:
                print("File info parser failed:", repr(e))
            for record in records:
                # TODO: Should files be handled differently?
                record = toolbox.dict_merge(record, file_info)
                output_queue.put(json.dumps(record))
    except Exception as e:
        print("DEBUG: Transformer error:", repr(e))

    return


def parse_crystal_structure(group, params=None):
    """Parser for the crystal_structure block.
    Will also populate material block.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict): N/A

    Returns:
    dict: The record parsed.
    """
    record = {}

    for data_file in group:
        material = {}
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

        # Parse material block
        material["composition"] = pmg_s.formula.replace(" ", "")
        # Parse crystal_structure block
        crystal_structure["space_group_number"] = pmg_s.get_space_group_info()[1]
        crystal_structure["number_of_atoms"] = int(pmg_s.composition.num_atoms)
        crystal_structure["volume"] = float(pmg_s.volume)

        # Add to record
        record = toolbox.dict_merge(record, {
                                                "material": material,
                                                "crystal_structure": crystal_structure
                                            })
    return record


def parse_tdb(group, params=None):
    record = {}

    for data_file in group:
        material = {}
        calphad = {}
        # Attempt to read the file
        try:
            calphad_db = pycalphad.Database(data_file)
            composition = ""
            for element in calphad_db.elements:
                if element.isalnum():
                    element = element.lower()
                    element = element[0].upper() + element[1:]
                    composition += element

            phases = list(calphad_db.phases.keys())

            material['composition'] = composition
            calphad['phases'] = phases

            # Add to record
            record = toolbox.dict_merge(record, {
                                               "material": material,
                                               "calphad": calphad
                                           })
            return record
        except Exception as e:
            return {}


def parse_pif(group, params=None):
    """Use Citrine's parsers."""
    if not params:
        return {}

    # Setup
    dc_md = params["dataset"]["dc"]
    cit_path = os.path.join(params["service_data"], "citrine")
    os.makedirs(cit_path, exist_ok=True)
    cit_manager = IngesterManager()
    mdf_records = []

    raw_pifs = cit_manager.run_extensions(group, include=None, exclude=[],
                                          args={"quality_report": False})
    if not raw_pifs:
        print("DEBUG: PIF no data")
        return {}
    if not isinstance(raw_pifs, list):
        raw_pifs = [raw_pifs]
    id_pifs = cit_utils.set_uids(raw_pifs)

    for pif in id_pifs:
        mdf_pif = _translate_pif(pif_to_feedstock(pif))
        if mdf_pif:
            mdf_records.append(mdf_pif)

        pif_name = (pif.uid or str(ObjectId())) + ".pif"
        pif_path = os.path.join(cit_path, pif_name)
        try:
            with open(pif_path, 'w') as pif_file:
                pif_dump(add_dc(pif, dc_md), pif_file)
        except Exception as e:
            try:
                os.remove(pif_path)
            except FileNotFoundError:
                pass

    return mdf_records


def parse_json(group, params=None):
    """Parser for JSON.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        parsers (dict):
            json (dict):
                mapping (dict): The mapping of mdf_fields: json_fields

    Returns:
    dict: The record(s) parsed.
    """
    try:
        mapping = params["parsers"]["json"]["mapping"]
        source_name = params["dataset"]["mdf"]["source_name"]
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        with open(file_path) as f:
            file_json = json.load(f)
        records.extend(_parse_json(file_json, mapping, source_name))
    return records


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
    list of dict: The record(s) parsed.
    """
    try:
        csv_params = params["parsers"]["csv"]
        mapping = csv_params["mapping"]
        source_name = params["dataset"]["mdf"]["source_name"]
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        df = pd.read_csv(file_path, delimiter=csv_params.get("delimiter", ","), na_values=NA_VALUES)
        records.extend(_parse_pandas(df, mapping, source_name))
    return records


def parse_yaml(group, params=None):
    """Parser for YAML files.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        parsers (dict):
            yaml (dict):
                mapping (dict): The mapping of mdf_fields: yaml_fields

    Returns:
    list of dict: The record(s) parsed.
    """
    try:
        mapping = params["parsers"]["yaml"]["mapping"]
        source_name = params["dataset"]["mdf"]["source_name"]
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        with open(file_path) as f:
            file_json = yaml.safe_load(f)
        records.extend(_parse_json(file_json, mapping, source_name))
    return records


def parse_excel(group, params=None):
    """Parser for MS Excel files.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        parsers (dict):
            excel (dict):
                mapping (dict): The mapping of mdf_fields: excel_headers
                na_values (list of str): Values to treat as N/A. Default NA_VALUES

    Returns:
    list of dict: The record(s) parsed.
    """
    try:
        excel_params = params["parsers"]["excel"]
        mapping = excel_params["mapping"]
        source_name = params["dataset"]["mdf"]["source_name"]
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        df = pd.read_excel(file_path, na_values=NA_VALUES)
        records.extend(_parse_pandas(df, mapping, source_name))
    return records


def parse_image(group, params=None):
    """Parse an image."""
    records = []
    for file_path in group:
        try:
            im = Image.open(file_path)
            records.append({
                "image": {
                    "width": im.width,
                    "height": im.height,
                    "format": im.format
                }
            })
        except Exception:
            pass
    return records


def parse_electron_microscopy(group, params=None):
    """Parse an electron microscopy image with hyperspy library."""
    records = []
    for file_path in group:
        try:
            data = hs.load(file_path).metadata.as_dictionary()
        except Exception:
            pass
        else:
            em = {}
            # Image mode is SEM, TEM, or STEM.
            # STEM is a subset of TEM.
            if "SEM" in data.get('Acquisition_instrument', {}).keys():
                inst = "SEM"
            elif "TEM" in data.get('Acquisition_instrument', {}).keys():
                inst = "TEM"
            else:
                inst = "None"
            em['beam_current'] = (data.get('Acquisition_instrument', {}).get(inst, {})
                                      .get('beam_current', None))
            em['beam_energy'] = (data.get('Acquisition_instrument', {}).get(inst, {})
                                     .get('beam_energy', None))
            em['magnification'] = (data.get('Acquisition_instrument', {}).get(inst, {})
                                       .get('magnification', None))
            em['microscope'] = (data.get('Acquisition_instrument', {}).get(inst, {})
                                    .get('microscope', None))
            em['image_mode'] = (data.get('Acquisition_instrument', {}).get(inst, {})
                                    .get('acquisition_mode', None))
            detector = (data.get('Acquisition_instrument', {}).get(inst, {})
                            .get('Detector', None))
            if detector:
                em['detector'] = next(iter(detector))

            # Remove None values
            for key, val in list(em.items()):
                if val is None:
                    em.pop(key)
            if em:
                records.append({
                    "electron_microscopy": em
                })
    return records


# List of all non-internal parsers
ALL_PARSERS = [
    parse_crystal_structure,
    parse_pif,
    parse_json,
    parse_csv,
    parse_yaml,
    parse_excel,
    parse_image,
    parse_electron_microscopy
]


def _parse_file_info(group, params=None):
    """File information parser.
    Populates the "files" block.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        parsers (dict):
            file (dict):
                globus_endpoint (str): Data file endpoint.
                http_host (str): Data file HTTP host.
                local_path (str): The path to the root of the files on the current machine.
                host_path (str): The path to the root on the hosting machine. Default local_path.

    Returns:
    list of dict: The record(s) parsed.
    """
    try:
        file_params = params["parsers"]["file"]
    except (KeyError, AttributeError):
        raise ValueError("File info parser params missing")
    try:
        globus_endpoint = file_params["globus_endpoint"]
    except (KeyError, AttributeError):
        raise ValueError("File info globus_endpoint missing")
    try:
        http_host = file_params["http_host"]
    except (KeyError, AttributeError):
        raise ValueError("File info http_host missing")
    try:
        local_path = file_params["local_path"]
    except (KeyError, AttributeError):
        raise ValueError("File info local_path missing")
    host_path = file_params.get("host_path", local_path)

    files = []
    for file_path in group:
        host_file = file_path.replace(local_path, host_path)
        with open(file_path, "rb") as f:
            md = {
                "globus": globus_endpoint + host_file,
                "data_type": magic.from_file(file_path),
                "mime_type": magic.from_file(file_path, mime=True),
                "url": http_host + host_file,
                "length": os.path.getsize(file_path),
                "filename": os.path.basename(file_path),
                "sha512": sha512(f.read()).hexdigest()
            }
        files.append(md)
    return {
        "files": files
    }


def _parse_pandas(df, mapping, source_name=None):
    """Parse a Pandas DataFrame."""
    csv_len = len(df.index)
    df_json = json.loads(df.to_json())

    records = []
    for index in range(csv_len):
        new_map = {}
        for path, value in _flatten_struct(mapping):
            new_map[path] = value + "." + str(index)
        records.extend(_parse_json(df_json, new_map))
    return records


def _parse_json(file_json, mapping, source_name=None):
    """Parse a JSON file."""
    # Handle lists of JSON documents as separate records
    if not isinstance(file_json, list):
        file_json = [file_json]

    records = []
    for data in file_json:
        record = {}
        # Get (path, value) pairs from the key structure
        # Loop over each
        for mdf_path, json_path in _flatten_struct(mapping):
            if source_name:
                mdf_path = mdf_path.replace("__custom", source_name)
                json_path = json_path.replace("__custom", source_name)
            try:
                value = _follow_path(data, json_path)
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


def _flatten_struct(struct, path=""):
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
            for p in _flatten_struct(val, path+"."+key):
                yield p
        else:
            yield ((path+"."+key).strip(". "), val)


def _follow_path(json_data, json_path):
    """Get the value in the data pointed to by the path."""
    value = json_data
    for field in json_path.split("."):
        value = value[field]
    return value


def _translate_pif(pif):
    """Translate the dict form of a PIF into an MDF record."""
    translations = {
        "dft": {
            "Converged": "converged",
            "XC_Functional": "exchange_correlation_functional",
            "Cutoff_Energy_eV": "cutoff_energy"
        }
    }
    record = {}
    for block, mapping in translations.items():
        new_block = {}
        for pif_field, mdf_field in mapping.items():
            if pif_field in pif.keys():
                new_block[mdf_field] = pif[pif_field]
        if new_block:
            record[block] = new_block
    return record
