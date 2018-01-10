import json
import os
from copy import deepcopy

from tqdm import tqdm

from mdf_toolbox import toolbox
from mdf_refinery import validator
from mdf_refinery.omniparser import omniparse


def omniconvert(input_path, addl_metadata={}, verbose=False):
    """Convert a dataset into MDF feedstock as best as possible.

    Arguments:
    input_path (str): The root path of the dataset.
    addl_metadata (dict): Extra metadata, already in the $source_name block.
    verbose (bool): If True, will print status messages. Default False.

    Returns:
    dict: record (dict or None): The converted file.
                                 None if the file failed to convert.
    """
    if verbose:
        print("Begin converting")
        # Index records
        success_count = 0
        failures = []
        for file_data in tqdm(toolbox.find_files(input_path), desc="Processing files", disable= not verbose):
            path = os.path.join(file_data["path"], file_data["filename"])
            rc_md = deepcopy(rc_md_template)
            res, info = omniparse(path, info=True)
            if res:
                rc_md[info["parser"]] = res
                record_metadata = rc_md
                if feedstock_path:
                    json.dump(record_metadata, feedstock_file)
                    feedstock_file.write("\n")
                else:
                    all_feedstock.append(record_metadata)
                success_count += 1
            else:
                failures.append(path)

    if verbose:
        print("Finished converting\nProcessed:", success_count, "\Failed:", len(failures))

    return {
        "records_processed": success_count,
        "num_failures": len(failures),
        "failed_files": failures,
        "feedstock": all_feedstock
        }

