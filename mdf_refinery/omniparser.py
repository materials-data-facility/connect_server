import ase.io
from mdf_toolbox import toolbox


def omniparse(data_file, parser_tags=[], info=False):
    """Parse a data file however possible.

    Arguments:
    data_file (file object): The data file.
    parser_tags (list of str): Run parsers with these tags.
                               Default [], which runs all parsers.
    info (bool): If True, will return a tuple of (results, info) (see "Returns").
                 If False, will just return results.
                 Default False.

    Returns:
    dict (if not info): The metadata parsed from the file. Will be empty if file could not be parsed.
    tuple of (dict, dict) (if info): The metadata, and a dict of parsing information.
    """
    # List of parsers as tuple(function, [tags])
    ALL_PARSERS = [
        {
            "parser": parse_ase,
            "tags": ["ase", "dft", "simulation"],
            "block": "material"
        }
    ]
    record = {}
    # Check all parsers
    for parser_dict in ALL_PARSERS:
        # If tags match (or no tags specified)
        if not parser_tags or any([tag in parser_tags for tag in parser_dict["tags"]]):
            try:
                parser_res = parser(data_file, stats=True)
            # Exception indicates no data parsed
            except Exception as e:
                pass
            else:
                record[parser_dict["block"]] = toolbox.dict_merge(
                                                record[parser_dict["block"]],
                                                parser_res)
    return data


def parse_ase(data_file):
    """Parser for data in ASE-readable formats.
    If ASE is incapable of reading the file and raises an exception,
    the (first) return dict == {} and total_num == 0 and failure_num == 1.

    Arguments:
    file_path (str): Path to the data file.

    Returns:
    dict: Useful data ASE could pull out of the file.
    """
    
    ase_template = {
#        "constraints" : None,              # No get()
#        "all_distances" : None,
#        "angular_momentum" : None,
#        "atomic_numbers" : None,
#        "cell" : None,
        "cell_lengths_and_angles" : None,
#        "celldisp" : None,
#        "center_of_mass" : None,
#        "charges" : None,
        "chemical_formula" : None,
#        "chemical_symbols" : None,
#        "dipole_moment" : None,
#        "forces" : None,
#        "forces_raw" : None,               # No get()
#        "initial_charges" : None,
#        "initial_magnetic_moments" : None,
#        "kinetic_energy" : None,
#        "magnetic_moment" : None,
#        "magnetic_moments" : None,
#        "masses" : None,
#        "momenta" : None,
#        "moments_of_inertia" : None,
#        "number_of_atoms" : None,
        "pbc" : None,
#        "positions" : None,
#        "potential_energies" : None,
#        "potential_energy" : None,
#        "potential_energy_raw" : None,     # No get()
#        "reciprocal_cell" : None,
#        "scaled_positions" : None,
#        "stress" : None,
#        "stresses" : None,
#        "tags" : None,
        "temperature" : None,
#        "total_energy" : None,
#        "velocities" : None,
        "volume" : None,
#        "filetype": None,                  # No get()
#        "num_frames": None,                # No get()
#        "num_atoms": None                  # No get()
        }

    # Read the file and process it if the reading succeeds
    result = ase.io.read(data_file)
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
        ase_dict["filetype"] = ase.io.formats.filetype(file_path)
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

    return ase_dict

