import os
# pycalphad and hyperspy imports require this env var set
os.environ["MPLBACKEND"] = "agg"
# pycalphad and hyperspy run into dlopen static TLS errors, so retry imports when failing
try:
    import pycalphad  # noqa: E402
except ImportError:
    import pycalphad
try:
    import hyperspy.api as hs  # noqa: E402
except ImportError:
    import hyperspy.api as hs  # noqa: E402

from hashlib import sha512  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
# import os  # noqa: E402
from queue import Empty  # noqa: E402
import re  # noqa: E402
import urllib  # noqa: E402

# E402: module level import not at top of file
import ase.io  # noqa: E402
from bson import ObjectId  # noqa: E402
import magic  # noqa: E402
import mdf_toolbox  # noqa: E402
import pandas as pd  # noqa: E402
from PIL import Image  # noqa: E402
import pymatgen  # noqa: E402
from pymatgen.io.ase import AseAtomsAdaptor as ase_to_pmg  # noqa: E402
from pif_ingestor.manager import IngesterManager  # noqa: E402
from pypif.obj import System  # noqa: E402
from pypif.pif import dump as pif_dump  # noqa: E402
from pypif_sdk.util import citrination as cit_utils  # noqa: E402
from pypif_sdk.interop.mdf import _to_user_defined as pif_to_feedstock  # noqa: E402
from pypif_sdk.interop.datacite import add_datacite as add_dc  # noqa: E402
import xmltodict  # noqa: E402
import yaml  # noqa: E402

from mdf_connect_server import CONFIG  # noqa: E402

# Additional NaN values for Pandas
NA_VALUES = ["", " "]

# Create new logger (extractors are multi-process)
logger = logging.getLogger(__name__)
logger.setLevel(CONFIG["LOG_LEVEL"])
logger.propagate = False
logfile_formatter = logging.Formatter("[{asctime}] [{levelname}] {message}",
                                      style='{', datefmt="%Y-%m-%d %H:%M:%S")
logfile_handler = logging.FileHandler(CONFIG["EXTRACTOR_ERROR_FILE"], mode='a')
logfile_handler.setFormatter(logfile_formatter)
logger.addHandler(logfile_handler)

# Log debug messages for all extractor events. Extremely spammy.
SUPER_DEBUG = False

# List of extractors at bottom


def run_extractors(input_queue, output_queue, queue_done, extract_params):
    """Extract data files.

    Returns:
    list of dict: The metadata extractd from the file.
                  Will be empty if no selected extractor can extract data.
    """
    source_id = extract_params.get("dataset", {}).get("mdf", {}).get("source_id", "unknown")
    try:
        # Extract each group from the queue
        # Exit loop when queue_done is True and no groups remain
        while True:
            # Fetch group from queue
            try:
                group_info = input_queue.get(timeout=5)
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
            for extractor_name in (group_info["extractors"] or ALL_EXTRACTORS.keys()):
                try:
                    specific_params = mdf_toolbox.dict_merge(extract_params or {},
                                                             group_info["params"])
                    extractor_res = ALL_EXTRACTORS[extractor_name](group=group_info["files"],
                                                                   params=specific_params)
                except Exception as e:
                    logger.warn(("{} Extractor {} failed with "
                                 "exception {}").format(source_id, extractor_name, repr(e)))
                else:
                    # If a list of one record was returned, treat as single record
                    # Eliminates [{}] from cluttering feedstock
                    # Filters one-record results from extractors that always return lists
                    if isinstance(extractor_res, list) and len(extractor_res) == 1:
                        extractor_res = extractor_res[0]
                    # Only process actual results
                    if extractor_res:
                        # If a single record was returned, merge with others
                        if isinstance(extractor_res, dict):
                            single_record = mdf_toolbox.dict_merge(single_record, extractor_res)
                        # If multiple records were returned, add to list
                        elif isinstance(extractor_res, list):
                            # Only add records with data
                            [multi_records.append(rec) for rec in extractor_res if rec]
                        # Else, panic
                        else:
                            raise TypeError(("Extractor '{p}' returned "
                                             "type '{t}'!").format(p=extractor_name,
                                                                   t=type(extractor_res)))
                        logger.debug("{}: {} extractd {}".format(source_id, extractor_name,
                                                                 group_info["files"]))
                    elif SUPER_DEBUG:
                        logger.debug("{}: {} could not extract {}".format(source_id, extractor_name,
                                                                          group_info))
            # Merge the single_record into all multi_records if both exist
            if single_record and multi_records:
                records = [mdf_toolbox.dict_merge(r, single_record) for r in multi_records if r]
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
                file_info = _extract_file_info(group=group_info["files"], params=extract_params)
            except Exception as e:
                logger.warning("{}: File info extractor failed: {}".format(source_id, repr(e)))
            for record in records:
                # TODO: Should files be handled differently?
                record = mdf_toolbox.dict_merge(record, file_info)
                output_queue.put(json.dumps(record))
    except Exception as e:
        logger.error("{}: Extractor error: {}".format(source_id, str(e)))
    # Log all exceptions!
    except BaseException as e:
        logger.error("{}: Extractor BaseException: {}".format(source_id, str(e)))
    return


def extract_crystal_structure(group, params=None):
    """Extractor for the crystal_structure block.
    Will also populate material block.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict): N/A

    Returns:
    dict: The record extractd.
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
        except Exception:
            try:
                # Read with Pymatgen
                pmg_s = pymatgen.Structure.from_file(data_file)
            except Exception:
                # Can't read file
                continue

        # Extract material block
        material["composition"] = pmg_s.formula.replace(" ", "")
        # Extract crystal_structure block
        crystal_structure["space_group_number"] = pmg_s.get_space_group_info()[1]
        crystal_structure["number_of_atoms"] = float(pmg_s.composition.num_atoms)
        crystal_structure["volume"] = float(pmg_s.volume)
        crystal_structure["stoichiometry"] = pmg_s.composition.anonymized_formula

        # Add to record
        record = mdf_toolbox.dict_merge(record, {
                                                "material": material,
                                                "crystal_structure": crystal_structure
                                            })
    return record


def extract_tdb(group, params=None):
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

            if composition:
                material['composition'] = composition
            if phases:
                calphad['phases'] = phases

        except Exception:
            pass
        else:
            # Add to record
            if material:
                record = mdf_toolbox.dict_merge(record, {"material": material})
            if calphad:
                record = mdf_toolbox.dict_merge(record, {"calphad": calphad})

    return record


def extract_pif(group, params=None):
    """Use Citrine's extractors."""
    if not params:
        return {}

    # Setup
    dc_md = params["dataset"]["dc"]
    cit_path = os.path.join(params["service_data"], "citrine")
    os.makedirs(cit_path, exist_ok=True)
    cit_manager = IngesterManager()
    mdf_records = []

    try:
        raw_pifs = cit_manager.run_extensions(group, include=params.get("include", None),
                                              exclude=[])
    except Exception as e:
        logger.debug("Citrine pif-ingestor raised exception: " + repr(e))
        return {}
    if not raw_pifs:
        return {}
    elif isinstance(raw_pifs, System):
        raw_pifs = [raw_pifs]
    elif not isinstance(raw_pifs, list):
        raw_pifs = list(raw_pifs)
    id_pifs = cit_utils.set_uids(raw_pifs)

    for pif in id_pifs:
        try:
            pif_feed = pif_to_feedstock(pif)
        except Exception as e:
            logger.warn("PIF to feedstock failed: " + repr(e))
            raise
        try:
            mdf_pif = _translate_pif(pif_feed)
        except Exception as e:
            logger.warn("_translate_pif failed: " + repr(e))
            raise
        if mdf_pif:
            mdf_records.append(mdf_pif)

        pif_name = (pif.uid or str(ObjectId())) + ".pif"
        pif_path = os.path.join(cit_path, pif_name)
        try:
            with open(pif_path, 'w') as pif_file:
                pif_dump(add_dc(pif, dc_md), pif_file)
        except Exception as e:
            logger.warn("Could not save PIF: {}".format(repr(e)))
            try:
                os.remove(pif_path)
            except FileNotFoundError:
                pass

    return mdf_records


def extract_json(group, params=None):
    """Extractor for JSON.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        extractors (dict):
            json (dict):
                mapping (dict): The mapping of mdf_fields: json_fields
                na_values (list of str): Values to treat as N/A. Default None.

    Returns:
    dict: The record(s) extractd.
    """
    try:
        mapping = params["extractors"]["json"]["mapping"]
        na_values = params["extractors"]["json"].get("na_values", None)
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        try:
            with open(file_path) as f:
                file_json = json.load(f)
        except Exception:
            pass
        else:
            records.extend(_extract_json(file_json, mapping, na_values=na_values))
    return records


def extract_csv(group, params=None):
    """Extractor for CSVs.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        extractors (dict):
            csv (dict):
                mapping (dict): The mapping of mdf_fields: csv_headers
                delimiter (str): The delimiter. Default ','
                na_values (list of str): Values to treat as N/A. Default NA_VALUES

    Returns:
    list of dict: The record(s) extractd.
    """
    try:
        csv_params = params["extractors"]["csv"]
        mapping = csv_params["mapping"]
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        try:
            df = pd.read_csv(file_path, delimiter=csv_params.get("delimiter", ","),
                             na_values=csv_params.get("na_values", NA_VALUES))
        except Exception:
            pass
        else:
            records.extend(_extract_pandas(df, mapping))
    return records


def extract_yaml(group, params=None):
    """Extractor for YAML files.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        extractors (dict):
            yaml (dict):
                mapping (dict): The mapping of mdf_fields: yaml_fields

    Returns:
    list of dict: The record(s) extractd.
    """
    try:
        mapping = params["extractors"]["yaml"]["mapping"]
        na_values = params["extractors"]["yaml"].get("na_values", None)
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        try:
            with open(file_path) as f:
                file_json = yaml.safe_load(f)
        except Exception:
            pass
        else:
            records.extend(_extract_json(file_json, mapping, na_values=na_values))
    return records


def extract_xml(group, params=None):
    """Extractor for XML files.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        extractors (dict):
            xml (dict):
                mapping (dict): The mapping of mdf_fields: xml_fields

    Returns:
    list of dict: The record(s) extractd.
    """
    try:
        mapping = params["extractors"]["xml"]["mapping"]
        na_values = params["extractors"]["xml"].get("na_values", None)
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        try:
            with open(file_path) as f:
                file_json = xmltodict.parse(f.read())
        except Exception:
            pass
        else:
            records.extend(_extract_json(file_json, mapping, na_values=na_values))
    return records


def extract_excel(group, params=None):
    """Extractor for MS Excel files.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        extractors (dict):
            excel (dict):
                mapping (dict): The mapping of mdf_fields: excel_headers
                na_values (list of str): Values to treat as N/A. Default NA_VALUES

    Returns:
    list of dict: The record(s) extractd.
    """
    try:
        excel_params = params["extractors"]["excel"]
        mapping = excel_params["mapping"]
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        try:
            df = pd.read_excel(file_path, na_values=excel_params.get("na_values", NA_VALUES))
        except Exception:
            pass
        else:
            records.extend(_extract_pandas(df, mapping))
    return records


def extract_image(group, params=None):
    """Extract an image."""
    records = []
    for file_path in group:
        try:
            im = Image.open(file_path)
            records.append({
                "image": {
                    "shape": [
                        im.height,
                        im.width,
                        len(im.getbands())
                    ]
                }
            })
        except Exception:
            pass
    return records


def extract_electron_microscopy(group, params=None):
    """Extract an electron microscopy image with hyperspy library."""
    records = []
    for file_path in group:
        try:
            hs_data = hs.load(file_path)
            data = hs_data.metadata.as_dictionary()
            raw_data = hs_data.original_metadata.as_dictionary()
        except Exception:
            pass
        else:
            em = {}
            image = {}
            # Image mode is SEM, TEM, or STEM.
            # STEM is a subset of TEM.
            if "SEM" in data.get('Acquisition_instrument', {}).keys():
                inst = "SEM"
            elif "TEM" in data.get('Acquisition_instrument', {}).keys():
                inst = "TEM"
            else:
                inst = "None"

            # HS data
            try:
                inst_data = data['Acquisition_instrument'][inst]
            except KeyError:
                pass
            else:
                try:
                    em['beam_energy'] = inst_data['beam_energy']
                except KeyError:
                    pass
                try:
                    em['magnification'] = inst_data['magnification']
                except KeyError:
                    pass
                try:
                    em['acquisition_mode'] = inst_data['acquisition_mode']
                except KeyError:
                    pass
                try:
                    detector = inst_data['Detector']
                except KeyError:
                    pass
                else:
                    em['detector'] = next(iter(detector))

            # Non-HS data (not pulled into standard HS metadata)
            # Pull out common dicts
            try:
                micro_info = raw_data["ImageList"]["TagGroup0"]["ImageTags"]["Microscope Info"]
            except KeyError:
                micro_info = {}
            try:
                exp_desc = raw_data["ObjectInfo"]["ExperimentalDescription"]
            except KeyError:
                exp_desc = {}

            # emission_current
            try:
                em["emission_current"] = micro_info["Emission Current (µA)"]
            except KeyError:
                try:
                    em["emission_current"] = exp_desc["Emission_uA"]
                except KeyError:
                    pass
            # operation_mode
            try:
                em["operation_mode"] = micro_info["Operation Mode"]
            except KeyError:
                pass
            # microscope
            try:
                em["microscope"] = (raw_data["ImageList"]["TagGroup0"]["ImageTags"]
                                            ["Session Info"]["Microscope"])
            except KeyError:
                try:
                    em["microscope"] = micro_info["Name"]
                except KeyError:
                    pass
            # spot_size
            try:
                em["spot_size"] = exp_desc["Spot size"]
            except KeyError:
                pass

            # Image metadata
            try:
                shape = []
                base_shape = list(raw_data["ImageList"]["TagGroup0"]
                                          ["ImageData"]["Dimensions"].values())
                # Reverse X and Y order to match MDF schema (y, x, z, ..., channels)
                if len(base_shape) >= 2:
                    shape.append(base_shape[1])
                    shape.append(base_shape[0])
                    shape.extend(base_shape[2:])
                # If 1 dimension, don't need to swap
                elif len(base_shape) > 0:
                    shape = base_shape

                if shape:
                    image["shape"] = shape
            except KeyError:
                pass

            # Remove None/empty values
            for key, val in list(em.items()):
                if val is None or val == [] or val == {}:
                    em.pop(key)
            record = {}
            if em:
                record["electron_microscopy"] = em
            if image:
                record["image"] = image
            if record:
                records.append(record)
    return records


def extract_filename(group, params=None):
    """Extractor for metadata stored in filenames.
    Will populate blocks according to mapping.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        extractors (dict):
            filename (dict):
                mapping (dict): The mapping of mdf_fields: regex_pattern

    Returns:
    list of dict: The record(s) extractd.
    """
    try:
        filename_params = params["extractors"]["filename"]
        mapping = filename_params["mapping"]
    except (KeyError, AttributeError):
        return {}

    records = []
    for file_path in group:
        record = {}
        filename = os.path.basename(file_path)
        for mdf_path, pattern in _flatten_struct(mapping):
            match = re.search(pattern, filename)
            if match:
                fields = mdf_path.split(".")
                last_field = fields.pop()
                current_field = record
                # Create all missing fields
                for field in fields:
                    if current_field.get(field) is None:
                        current_field[field] = {}
                    current_field = current_field[field]
                # Add value to end
                current_field[last_field] = match.group()
        if record:
            records.append(record)
    return records


ALL_EXTRACTORS = {
    "crystal_structure": extract_crystal_structure,
    "tdb": extract_tdb,
    "pif": extract_pif,
    "json": extract_json,
    "csv": extract_csv,
    "yaml": extract_yaml,
    "xml": extract_xml,
    "excel": extract_excel,
    "image": extract_image,
    "electron_microscopy": extract_electron_microscopy,
    "filename": extract_filename
}


def _extract_file_info(group, params=None):
    """File information extractor.
    Populates the "files" block.

    Arguments:
    group (list of str): The paths to grouped files.
    params (dict):
        extractors (dict):
            file (dict):
                globus_endpoint (str): Data file endpoint.
                http_host (str): Data file HTTP host.
                local_path (str): The path to the root of the files on the current machine.

    Returns:
    list of dict: The record(s) extractd.
    """
    try:
        globus_host_info = urllib.parse.urlparse(params["extractors"]["file"]["globus_host"])
        host_endpoint = globus_host_info.netloc
        host_path = globus_host_info.path
    except Exception as e:
        raise ValueError("File info host_endpoint missing or corrupted: {}".format(str(e)))
    try:
        http_host = params["extractors"]["file"]["http_host"]
    except Exception:
        raise ValueError("File info http_host missing")
    try:
        local_path = params["extractors"]["file"]["local_path"]
    except Exception:
        raise ValueError("File info local_path missing")

    files = []
    for file_path in group:
        host_file = file_path.replace(local_path, host_path)
        with open(file_path, "rb") as f:
            md = {
                "globus": "globus://{}{}".format(host_endpoint, host_file),
                "data_type": magic.from_file(file_path),
                "mime_type": magic.from_file(file_path, mime=True),
                "url": (http_host + host_file) if http_host else None,
                "length": os.path.getsize(file_path),
                "filename": os.path.basename(file_path),
                "sha512": sha512(f.read()).hexdigest()
            }
        files.append(md)
    return {
        "files": files
    }


def _extract_pandas(df, mapping):
    """Extract a Pandas DataFrame."""
    csv_len = len(df.index)
    df_json = json.loads(df.to_json())

    records = []
    for index in range(csv_len):
        new_map = {}
        for path, value in _flatten_struct(mapping):
            new_map[path] = value + "." + str(index)
        records.extend(_extract_json(df_json, new_map))
    return records


def _extract_json(file_json, mapping, na_values=None):
    """Extract a JSON file."""
    # Handle lists of JSON documents as separate records
    if not isinstance(file_json, list):
        file_json = [file_json]
    if na_values is None:
        na_values = []
    elif not isinstance(na_values, list):
        na_values = [na_values]

    records = []
    for data in file_json:
        record = {}
        # Get (path, value) pairs from the key structure
        # Loop over each
        for mdf_path, json_path in _flatten_struct(mapping):
            try:
                value = _follow_path(data, json_path)
            except KeyError:
                value = None
            # Only add value if value exists and is not N/A
            if value is not None and value not in na_values:
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
    # block: { PIF field: (MDF field, translation function) }
    translations = {
        "dft": {
            "Converged": ("converged", bool),
            "XC_Functional": ("exchange_correlation_functional", str),
            "Cutoff_Energy_eV": ("cutoff_energy", float)
        },
        "crystal_structure": {
            "Space_group_number": ("space_group_number", int),
            "Number_of_atoms_in_unit_cell": ("number_of_atoms", float),
            "Unit_cell_volume_AA_3": ("volume", float)
        }
    }
    record = {}
    for block, mapping in translations.items():
        new_block = {}
        for pif_field, mdf_field_info in mapping.items():
            mdf_field = mdf_field_info[0]
            translator = mdf_field_info[1]
            if pif_field in pif.keys():
                new_block[mdf_field] = translator(pif[pif_field])
        if new_block:
            record[block] = new_block
    return record
