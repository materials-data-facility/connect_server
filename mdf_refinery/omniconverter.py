import json
import os
from copy import deepcopy

from tqdm import tqdm

from mdf_forge import toolbox
from mdf_refinery import validator

from omniparser import omniparse


def omniconvert(input_path, all_metadata, feedstock_path, verbose=False):
    """Convert a dataset into MDF feedstock as best as possible.

    Arguments:
    input_path (str): The root path of the dataset.
    all_metadata (dict): The metadata for the dataset and record entries.
        This dict should be composed of the keys "dataset" and "record", for each
        metadata type.
    feedstock_path (str): File to store feedstock.
                          If None, will return feedstock instead of writing out.
    verbose (bool): If True, will print status messages. Default False.

    Returns:
    None (if feedstock_path is not None)
    dict (if feedstock_path is None): The feedstock.

    Outputs:
    Uses the Validator to output feedstock into the appropriate directory.
    """
    if verbose:
        print("Begin converting")

    ds_md = {
        "dc": all_metadata.get("dc"),
        "mdf": all_metadata.get("mdf")
        }
    rc_md_template = {
        "mdf": all_metadata.get("mdf"),
        "files": [],
        "materials": {}
        }
    source_name = all_metadata.get("mdf", {}).get("source_name")

    all_feedstock = None
    with open(feedstock_path or os.devnull, 'w') as feedstock_file:
        # Handle dataset entry
        dataset_result = validator.validate_dataset(ds_md)
        if not dataset_result["success"]:
            raise ValueError(dataset_result["error"])
        dataset_metadata = dataset_result["valid"]
        if feedstock_path:
            json.dump(dataset_metadata, feedstock_file)
            feedstock_file.write("\n")
        else:
            all_feedstock = [dataset_metadata]

        # Index records
        success_count = 0
        failures = []
        for file_data in tqdm(toolbox.find_files(input_path), desc="Processing files", disable= not verbose):
            path = os.path.join(file_data["path"], file_data["filename"])
            rc_md = deepcopy(rc_md_template)
            res, info = omniparse(path, info=True)
            if res:
                rc_md[info["parser"]] = res
                record_result = validator.validate_record(rc_md)
                if not record_result["success"]:
                    raise ValueError(record_result["error"])
                record_metadata = record_result["valid"]
                if feedstock_path:
                    json.dump(record_metadata, feedstock_file)
                    feedstock_file.write("\n")
                else:
                    all_feedstock.append(record_metadata)


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

