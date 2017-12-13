import json

import ase.io
from mdf_toolbox import toolbox
import pandas as pd


# List of parsers at bottom


def omniparse(data_file, special_formats=None):
    """Parse a data file however possible.

    Arguments:
    data_file (file object or str): The data file or path.
    special_formats (dict): Run parsers with these parameters. Default None.

    Returns:
    list of dict: The metadata parsed from the file.
                  Will be empty if no selected parser can parse data.
    """
    if special_formats is None:
        special_formats = {}
    records = []

    # Open data_file if necessary
    # Wrap in a try-finally to always close file
    # with does not provide enough conditional functionality
    try:
        if isinstance(data_file, str):
            file_data = open(data_file)
        else:
            file_data = data_file
        # Check all parsers
        for par_name, par_func in ALL_PARSERS.items():
            # All parsers should be run if "exclusive" is set
            # Otherwise, only parsers listed in the formats should be run
            if "exclusive" not in special_formats.keys() or par_name in special_formats.keys():
                try:
                    # Call parser with params if present
                    parser_res = par_func(file_data, params=special_formats.get(par_name, None))
                    # If no data returned, fail
                    if not parser_res:
                        raise ValueError("No data parsed")
                    # If single record returned, make into list
                    elif not isinstance(parser_res, list):
                        parser_res = [parser_res]
                # Exception indicates no data parsed
                except Exception as e:
                    pass
                else:
                    # TODO: Challenge assumption:
                    #       All parsers return same number of records or fail
                    if len(records) == 0:
                        records = parser_res
                    else:
                        new_records = []
                        for r1, r2 in zip(records, parser_res):
                            new_records.append(toolbox.dict_merge(r1, r2))
                        records = new_records
                file_data.seek(0)
    finally:
        # Close file if opened in this function
        if isinstance(data_file, str):
            file_data.close()
    return records


def parse_ase(file_data, **ignored):
    """Parser for data in ASE-readable formats.
    If ASE is incapable of reading the file, an exception will be raised.

    Arguments:
    file_data (file object or str): Data file, or path to the data file.
    ignored (any): Ignored.

    Returns:
    dict: Useful data ASE could pull out of the file.
    """
    ase_template = {
        # "constraints": None,              # No get()
        # "all_distances": None,
        # "angular_momentum": None,
        # "atomic_numbers": None,
        # "cell": None,
        "cell_lengths_and_angles": None,
        # "celldisp": None,
        # "center_of_mass": None,
        # "charges": None,
        "chemical_formula": None,
        # "chemical_symbols": None,
        # "dipole_moment": None,
        # "forces": None,
        # "forces_raw": None,               # No get()
        # "initial_charges": None,
        # "initial_magnetic_moments": None,
        # "kinetic_energy": None,
        # "magnetic_moment": None,
        # "magnetic_moments": None,
        # "masses": None,
        # "momenta": None,
        # "moments_of_inertia": None,
        # "number_of_atoms": None,
        "pbc": None,
        # "positions": None,
        # "potential_energies": None,
        # "potential_energy": None,
        # "potential_energy_raw": None,     # No get()
        # "reciprocal_cell": None,
        # "scaled_positions": None,
        # "stress": None,
        # "stresses": None,
        # "tags": None,
        "temperature": None,
        # "total_energy": None,
        # "velocities": None,
        "volume": None,
        # "filetype": None,                  # No get()
        # "num_frames": None,                # No get()
        # "num_atoms": None                  # No get()
        }

    # Read the file and process it if the reading succeeds
    result = ase.io.read(file_data)
    if not result:
        raise ValueError("No data")

    ase_dict = ase_template.copy()
    # Data with easy .get() functions
    for key in ase_dict.keys():
        try:
            ase_dict[key] = eval("result.get_" + key + "()")
        # Exceptions can be generally ignored
        except Exception as e:
            pass

    # Data without a .get()
    try:
        ase_dict["filetype"] = ase.io.formats.filetype(file_data)
    except Exception as e:
        pass
    try:
        ase_dict["num_atoms"] = len(result)
    except Exception as e:
        pass
#        if type(result) is list:
#            ase_dict["num_frames"] = len(result)
#        else:
#            ase_dict["num_atoms"] = len(result)

    # Fix up the extracted data
    none_keys = []
    for key in ase_dict.keys():
        # numpy ndarrays aren't JSON serializable
        if 'numpy' in str(type(ase_dict[key])).lower():
            ase_dict[key] = ase_dict[key].tolist()

        # None values aren't useful
        if ase_dict[key] is None:
            none_keys.append(key)
        # Remake lists with valid values
        elif type(ase_dict[key]) is list:
            new_list = []
            for elem in ase_dict[key]:
                # FixAtoms aren't JSON serializable
                if 'fixatoms' in str(elem).lower():
                    new_elem = elem.get_indices().tolist()
                else:
                    new_elem = elem
                # Only add elements with data
                if new_elem:
                    new_list.append(new_elem)
            # Only add lists with data
            if new_list:
                ase_dict[key] = new_list
            else:
                none_keys.append(key)
    # None keys aren't useful
    for key in none_keys:
        ase_dict.pop(key)

    if not ase_dict:
        raise ValueError("All data None")

    # Return correct block
    return {
        "materials": ase_dict
    }


def parse_csv(file_data, params=None):
    """Parse a CSV."""
    if not params:
        return {}
    df = pd.read_csv(file_data, na_values=["", " "])
    csv_len = len(df.index)
    df_json = json.loads(df.to_json())

    records = []
    for index in range(csv_len):
        new_struct = {}
        for path, value in flatten_struct(params):
            new_struct[path] = value + "." + str(index)
        rec = parse_json(df_json, new_struct)
        if rec:
            records.append(rec)
    return records


def parse_json(file_data, params=None):
    """Parse a JSON file."""
    # If no structure is supplied, do no parsing
    if not params:
        return {}
    record = {}
    if not isinstance(file_data, dict):
        data = json.load(file_data)
    else:
        data = file_data

    # Get (path, value) pairs from the key structure
    # Loop over each
    for mdf_path, json_path in flatten_struct(params):
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

    return record


# Dict of all parsers as parser:function
ALL_PARSERS = {
    "ase": parse_ase,
    "csv": parse_csv,
    "json": parse_json
}


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
