import os

import ase
import mdf_toolbox
import pymatgen


def parse(group, **params):
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

        # Parse material block
        material["composition"] = pmg_s.formula.replace(" ", "") 
        # Parse crystal_structure block
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


def is_valid(group, **params):
    if isinstance(group, str):
        group = [group]
    try:
        result = parse(group, **params)
        if not result:
            raise ValueError()
    except Exception:
        return False
    else:
        return True


def group(root):
    if os.path.isfile(root):
        return root
    elif os.path.isdir(root):
        for path, dirs, files in os.walk(root):
            for f in files:
                yield f
    else:
        raise ValueError("Unknown path '{}'".format(root))

