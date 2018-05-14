from ctypes import c_bool
import json
import logging
import multiprocessing
import os
from queue import Empty

from mdf_connect import transform

NUM_TRANSFORMERS = 1

GROUPING_RULES = {
    "vasp": [
        "outcar",
        "incar",
        "chgcar",
        "wavecar",
        "wavcar",
        "ozicar",
        "ibzcar",
        "kpoints",
        "doscar",
        "poscar",
        "contcar",
        "vasp_run.xml",
        "xdatcar"
    ]
}

logger = logging.getLogger(__name__)


def convert(root_path, convert_params):
    """Convert files under the root path into feedstock.

    Arguments:
    root_path (str): The path to the directory holding all the dataset files.
    convert_params (dict): Parameters for conversion.
        dataset (dict): The dataset associated with the files.
        parsers (dict): Parser-specific parameters, keyed by parser (ex. "json": {...}).
        service_data (str): The path to a directory to store integration data.

    Returns:
    list of dict: The full feedstock for this dataset, including dataset entry.
    """
    source_id = convert_params.get("dataset", {}).get("mdf", {}).get("source_id", "unknown")
    # Set up multiprocessing
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    input_complete = multiprocessing.Value(c_bool, False)

    # Start up transformers
    transformers = [multiprocessing.Process(target=transform,
                                            args=(input_queue, output_queue,
                                                  input_complete, convert_params))
                    for i in range(NUM_TRANSFORMERS)]
    [t.start() for t in transformers]
    logger.debug("{}: Transformers started".format(source_id))

    # Populate input queue
    num_groups = 0
    for group in group_tree(root_path):
        input_queue.put(group)
        num_groups += 1
    # Mark that input is finished
    input_complete.value = True
    logger.debug("{}: Input complete".format(source_id))

    # TODO: Process dataset entry
    full_dataset = convert_params["dataset"]

    # Create complete feedstock
    feedstock = [full_dataset]
    while True:
        try:
            record = output_queue.get(timeout=1)
            feedstock.append(json.loads(record))
        except Empty:
            if any([t.is_alive() for t in transformers]):
                [t.join(timeout=1) for t in transformers]
            else:
                logger.debug("{}: Transformers joined".format(source_id))
                break

    return (feedstock, num_groups)


def group_tree(root):
    """Group files based on format-specific rules."""
    for path, dirs, files in os.walk(os.path.abspath(root)):
        groups = []
        # TODO: Expand grouping formats
        # File-matching groups
        # Each format specified in the rules
        for format_type, format_name_list in GROUPING_RULES.items():
            format_groups = {}
            # Check each file for rule matching
            # Match to appropriate group (with same pre/post pattern)
            #   eg a_[match]_b groups with a_[other match]_b but not c_[other match]_d
            for f in files:
                fname = f.lower().strip()
                for format_name in format_name_list:
                    if format_name in fname:
                        pre_post_pattern = fname.replace(format_name, "")
                        if not format_groups.get(pre_post_pattern):
                            format_groups[pre_post_pattern] = []
                        format_groups[pre_post_pattern].append(f)
                        break
            # Remove grouped files from the file list and add groups to the group list
            for g in format_groups.values():
                for f in g:
                    files.remove(f)
                groups.append(g)

        # NOTE: Keep this grouping last!
        # Default grouping: Each file is a group
        groups.extend([[f] for f in files])

        # Add path to filenames and yield each group
        for g in groups:
            yield [os.path.join(path, f) for f in g]
