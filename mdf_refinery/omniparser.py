import json

import ase.io
from mdf_toolbox import toolbox
import pandas as pd
from PIL import Image

# data_format to data_type translations
FORMAT_TYPE = {
    "vasp": "dft"
}

# Additional NaN values for Pandas
NA_VALUES = ["", " "]

# List of parsers at bottom
# All parsers accept data_path and/or file_data, and arbitrary other parameters


def omniparse(data_paths, parse_params=None):
    """Parse data files however possible.

    Arguments:
    data_paths (str or list of str): The path(s) to the data file(s).
    parse_params (dict): Run parsers with these parameters. Default None.

    Returns:
    list of dict: The metadata parsed from the file.
                  Will be empty if no selected parser can parse data.
    """
    if parse_params is None:
        parse_params = {}
    if isinstance(data_paths, str):
        data_paths = [data_paths]

    records = []

    # Parse each data file
    for path in data_paths:
        # Open data_file
        with open(path) as file_data:
            # Check all parsers
            for par_name, par_func in ALL_PARSERS.items():
                # All parsers should be run if "exclusive" is set
                # Otherwise, only parsers listed in the formats should be run
                if not parse_params.get("exclusive") or par_name in parse_params.keys():
                    try:
                        # Call parser with params if present
                        parser_res = par_func(data_path=path,
                                              file_data=file_data,
                                              params=parse_params.get(par_name, None))
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
                        # If only one file is being parsed, return all records
                        if len(data_paths) == 1:
                            records = parser_res
                        # If multiple files are being parsed, merge results
                        else:
                            # All results should be merged into records[0]
                            if len(records) == 0:
                                records.append({})
                            for res in parser_res:
                                records[0] = toolbox.dict_merge(records[0], res)
                    file_data.seek(0)
    return records


def parse_ase(data_path=None, **ignored):
    """Parser for data in ASE-readable formats.
    If ASE is incapable of reading the file, an exception will be raised.

    Arguments:
    data_path (str): Path to the data file.
    ignored (any): Ignored arguments.

    Returns:
    dict: Useful data ASE could pull out of the file.
    """
    ase_formats = {
        'abinit': "abinit",
        'aims': "aims",
        'aims-output': "aims",
        'bundletrajectory': "bundletrajectory",
        'castep-castep': "castep",
        'castep-cell': "castep",
        'castep-geom': "castep",
        'castep-md': "castep",
        'castep-phonon': "castep",
        'cfg': "atomeye",
        'cif': "cif",
        'cmdft': "cmdft",
        'cube': "cube",
        'dacapo': "dacapo",
        'dacapo-text': "dacapo",
        'db': "ase_db",
        'dftb': "dftb",
        'dlp4': "dlp4",
        'dmol-arc': "dmol3",
        'dmol-car': "dmol3",
        'dmol-incoor': "dmol3",
        'elk': "elk",
        'eon': "eon",
        'eps': "eps",
        'espresso-in': "espresso",
        'espresso-out': "espresso",
        'etsf': "etsf",
        'exciting': "exciting",
        'extxyz': "extxyz",
        'findsym': "findsym",
        'gaussian': "gaussian",
        'gaussian-out': "gaussian",
        'gen': "dftb",
        'gpaw-out': "gpaw",
        'gpw': "gpaw",
        'gromacs': "gromacs",
        'gromos': "gromos",
        'html': "html",
        'iwm': "iwn",
        'json': "ase_json",
        'jsv': "jsv",
        'lammps-dump': "lammps",
        'lammps-data': "lammps",
        'magres': "magres",
        'mol': "mol",
        'nwchem': "nwchem",
        'octopus': "octopus",
        'proteindatabank': "proteindatabank",
        'png': "ase_png",
        'postgresql': "ase_postgresql",
        'pov': "pov",
        'py': "ase_py",
        'qbox': "qbox",
        'res': "shelx",
        'sdf': "sdf",
        'struct': "wien2k",
        'struct_out': "siesta",
        'traj': "ase_traj",
        'trj': "ase_trj",
        'turbomole': "turbomole",
        'turbomole-gradient': "turbomole",
        'v-sim': "v-sim",
        'vasp': "vasp",
        'vasp-out': "vasp",
        'vasp-xdatcar': "vasp",
        'vasp-xml': "vasp",
        'vti': "vtk",
        'vtu': "vtk",
        'x3d': "x3d",
        'xsd': "xsd",
        'xsf': "xsf",
        'xyz': "xyz"
        }
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
    record = {}
    materials = {}
    # Read the file and process it if the reading succeeds
    # Will throw exception on certain failures
    result = ase.io.read(data_path)
    if not result:
        raise ValueError("No data")

    # Must have a known data format to know which block to output to
    try:
        materials["data_format"] = ase_formats[ase.io.formats.filetype(data_path)]
    except Exception as e:
        raise ValueError("Unable to determine data format.")
    materials["data_type"] = FORMAT_TYPE[materials["data_format"]]

    try:
        composition = result.get_chemical_formula()
        materials["composition"] = composition
    except Exception as e:
        # No composition extracted
        pass

    '''
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
        ase_dict["num_atoms"] = len(result)
    except Exception as e:
        pass
#        if type(result) is list:
#            ase_dict["num_frames"] = len(result)
#        else:
#            ase_dict["num_atoms"] = len(result)

    # Format fields
    ase_dict["composition"] = ase_dict.pop("chemical_formula", None)

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
    '''

    # Return correct record
    if materials:
        record["materials"] = materials
    return record


def parse_csv(file_data=None, params=None, **ignored):
    """Parse a CSV."""
    if not params or not file_data:
        return {}
    df = pd.read_csv(file_data, delimiter=params.pop("delimiter", ","), na_values=NA_VALUES)
    return parse_pandas(df, params.get("mapping", {}))


def parse_excel(file_data=None, params=None, **ignored):
    """Parse an Excel file."""
    if not params or not file_data:
        return {}
    df = pd.read_excel(file_data, na_values=NA_VALUES)
    return parse_pandas(df, params.get("mapping", {}))


def parse_hdf5(file_data=None, params=None, **ignored):
    """Parse an HDF5 file."""
    if not params or not file_data:
        return {}
    df = pd.read_hdf(file_data)
    return parse_pandas(df, params.get("mapping", {}))


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


# Dict of all user-selectable parsers as parser:function
ALL_PARSERS = {
    "ase": parse_ase,
    "csv": parse_csv,
    "excel": parse_excel,
    "hdf5": parse_hdf5,
    "json": parse_json,
    "jpg": parse_image,
    "png": parse_image
}


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
