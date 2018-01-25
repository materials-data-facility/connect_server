from ctypes import c_bool
import json
import multiprocessing
import os
from queue import Empty

from mdf_refinery import transform

NUM_TRANSFORMERS = 5


def convert(root_path, convert_params):
    """Convert files under the root path into feedstock.

    Arguments:
    root_path (str): The path to the directory holding all the dataset files.
    dataset (dict): The current dataset entry.
    convert_params (dict): Parameters for conversion.

    Returns:
    list of dict: The full feedstock for this dataset, including dataset entry.
    """
    # Set up multiprocessing
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    input_complete = multiprocessing.Value(c_bool, False)

    parse_params = {
        "dataset": convert_params,
        "parsers": convert_params.pop("index", {})
    }

    # Start up transformers
    transformers = [multiprocessing.Process(target=transform,
                                            args=(input_queue, output_queue, 
                                                  input_complete, parse_params))
                    for i in range(NUM_TRANSFORMERS)]
    [t.start() for t in transformers]
    print("DEBUG: Transformers started")

    # Populate input queue
    [input_queue.put(group) for group in group_tree(root_path)]
    # Mark that input is finished
    input_complete.value = True
    print("DEBUG: Input complete")

    # TODO: Process dataset entry
    full_dataset = convert_params

    # Wait for transformers
    [t.join() for t in transformers]
    print("DEBUG: Transformers joined")

    # Create complete feedstock
    feedstock = [full_dataset]
    while True:
        try:
            record = output_queue.get(timeout=1)
            feedstock.append(json.loads(record))
        except Empty:
            break

    return feedstock


def group_tree(root):
    """Group files based on format-specific rules."""
    for path, dirs, files in os.walk(os.path.abspath(root)):
        groups = []
        # TODO: Expand list of triggers
        # VASP
        # TODO: Use regex instead of exact matching
        if "OUTCAR" in files:
            outcar_files = ["OUTCAR", "INCAR", "POSCAR", "WAVCAR"]
            new_group = []
            for group_file in outcar_files:
                # Remove file from list and add to group if present
                # If not present, noop
                try:
                    files.remove(group_file)
                except ValueError:
                    pass
                else:
                    new_group.append(group_file)
            if new_group:  # Should always be present
                groups.append(new_group)

        # NOTE: Keep this grouping last!
        # Each file group
        groups.extend([[f] for f in files])

        # Add path to filenames and yield each group
        for g in groups:
            yield [os.path.join(path, f) for f in g]
        #[yield [os.path.join(path, f) for f in g] for g in groups]
