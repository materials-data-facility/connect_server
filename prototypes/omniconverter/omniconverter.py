import json
import os
from copy import deepcopy

from tqdm import tqdm

from mdf_forge import toolbox
from mdf_refinery.validator import Validator

from omniparser import omniparse


def omniconvert(input_path, all_metadata, verbose=False):
    """Convert a dataset into MDF feedstock as best as possible.

    Arguments:
    input_path (str): The root path of the dataset.
    all_metadata (dict): The metadata for the dataset and record entries.
        This dict should be composed of the keys "dataset" and "record", for each
        metadata type.
    verbose (bool): If True, will print status messages. Default False.

    Returns:
    None

    Outputs:
    Uses the Validator to output feedstock into the appropriate directory.
    """
    if verbose:
        print("Begin converting")

    dataset_validator = Validator(all_metadata["dataset"])
    source_name = all_metadata["dataset"]["mdf"]["source_name"]
    all_metadata["record"][source_name] = all_metadata["record"].get(source_name, {})

    success_count = 0
    failures = []
    for file_data in tqdm(toolbox.find_files(input_path), desc="Processing files", disable= not verbose):
        path = os.path.join(file_data["path"], file_data["filename"])
        record_metadata = deepcopy(all_metadata["record"])
        res, info = omniparse(path, info=True)
        if res:
            record_metadata[source_name].update(res)
            val = dataset_validator.write_record(record_metadata)
            if not val["success"]:
                if not dataset_validator.cancel_validation()["success"]:
                    print("Error cancelling validation. The partial feedstock may not be removed.")
                raise ValueError(val["message"] + "\n" + val.get("details", ""))
            success_count += 1
        else:
            failures.append(path)

    if verbose:
        print("Finished converting\nProcessed:", success_count, "\Failed:", len(failures))

    return {
        "records_processed": success_count,
        "files_failed": len(failures),
        "failed_files": failures
        }

