from ctypes import c_bool
import json
import logging
import multiprocessing
import os
from queue import Empty

import mdf_toolbox

from mdf_connect_server import CONFIG, utils
from mdf_connect_server.processor import transform, Validator


logger = logging.getLogger(__name__)


def convert(root_path, convert_params):
    """Convert files under the root path into feedstock.

    Arguments:
    root_path (str): The path to the directory holding all the dataset files.
    convert_params (dict): Parameters for conversion.
        dataset (dict): The dataset associated with the files.
        parsers (dict): Parser-specific parameters, keyed by parser (ex. "json": {...}).
        service_data (str): The path to a directory to store integration data.
        feedstock_file (str): Path to output feedstock to.
        group_config (dict): Grouping configuration.
        validation_info (dict): Validator configuration. Default None.

    Returns:
    dict: The results.
        success (bool): False if the conversion failed to complete. True otherwise.
        error (str): If success is False, the error encountered.
        dataset (dict): If success is True, the dataset entry.
        num_records (int): If success is True, the number of records parsed.
        num_groups (int): If success is True, the number of parsed groups.
        extensions (list of str): If success is True, all unique file extensions in the dataset.
    """
    source_id = convert_params.get("dataset", {}).get("mdf", {}).get("source_id", "unknown")
    source_info = utils.split_source_id(source_id)
    vald = Validator(schema_path=CONFIG["SCHEMA_PATH"])

    # Process dataset entry (to fail validation early if dataset entry is invalid)
    full_dataset = convert_params["dataset"]
    # Fetch custom block descriptors, cast values to str
    new_custom = {}
    # custom block descriptors
    # Turn _description => _desc
    for key, val in full_dataset.pop("custom", {}).items():
        if key.endswith("_description"):
            new_custom[key[:-len("ription")]] = str(val)
        else:
            new_custom[key] = str(val)
    for key, val in full_dataset.pop("custom_desc", {}).items():
        if key.endswith("_desc"):
            new_custom[key] = str(val)
        elif key.endswith("_description"):
            new_custom[key[:-len("ription")]] = str(val)
        else:
            new_custom[key+"_desc"] = str(val)
    if new_custom:
        full_dataset["custom"] = new_custom

    # Validate dataset
    ds_res = vald.start_dataset(full_dataset, source_info,
                                convert_params.get("validation_info", None))
    if not ds_res["success"]:
        return ds_res

    # Set up multiprocessing
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    input_complete = multiprocessing.Value(c_bool, False)

    # Start up transformers
    transformers = [multiprocessing.Process(target=transform,
                                            args=(input_queue, output_queue,
                                                  input_complete, convert_params))
                    for i in range(CONFIG["NUM_TRANSFORMERS"])]
    [t.start() for t in transformers]
    logger.debug("{}: Transformers started".format(source_id))

    # Populate input queue
    num_groups = 0
    extensions = set()
    for group_info in group_tree(root_path, convert_params["group_config"]):
        input_queue.put(group_info)
        num_groups += 1
        for f in group_info["files"]:
            filename, ext = os.path.splitext(f)
            extensions.add(ext or filename)
    # Mark that input is finished
    input_complete.value = True
    logger.debug("{}: Input complete".format(source_id))

    # Create complete feedstock
    while True:
        try:
            record = output_queue.get(timeout=1)
            rc_res = vald.add_record(json.loads(record))
            # If one record fails, entire feedstock fails
            # So if a failure occurs, terminate all transformers and return
            if not rc_res["success"]:
                logger.info("{}: Record error - terminating transformers".format(source_id))
                # TODO: Use t.kill() (Py3.7-only)
                [t.terminate() for t in transformers]
                [t.join() for t in transformers]
                logger.debug("{}: Transformers terminated".format(source_id))

                return rc_res

        except Empty:
            if any([t.is_alive() for t in transformers]):
                [t.join(timeout=1) for t in transformers]
            else:
                logger.debug("{}: Transformers joined".format(source_id))
                break

    # Output feedstock
    os.makedirs(os.path.dirname(convert_params["feedstock_file"]), exist_ok=True)
    with open(convert_params["feedstock_file"], 'w') as out:
        feedstock_generator = vald.get_finished_dataset()
        # First entry is dataset
        dataset = next(feedstock_generator)
        json.dump(dataset, out)
        out.write("\n")
        # Subsequent entries are records
        num_records = 0
        for record in feedstock_generator:
            json.dump(record, out)
            out.write("\n")
            num_records += 1

    return {
        "success": True,
        "dataset": dataset,
        "num_records": num_records,
        "num_groups": num_groups,
        "extensions": list(extensions)
    }


def group_tree(root, config):
    """Run group_files on files in tree appropriately."""
    files = []
    dirs = []
    if root == "/dev/null":
        return []
    for node in os.listdir(root):
        node_path = os.path.join(root, node)
        if node == "mdf.json":
            with open(node_path) as f:
                try:
                    new_config = json.load(f)
                    logger.debug("Config updating: \n{}".format(new_config))
                except Exception as e:
                    logger.warning("Error reading config file '{}': {}".format(node_path, str(e)))
                else:
                    config = mdf_toolbox.dict_merge(new_config, config)
        elif os.path.isfile(node_path):
            files.append(node_path)
        elif os.path.isdir(node_path):
            dirs.append(node_path)
        else:
            logger.debug("Ignoring non-file, non-dir node '{}'".format(node_path))

    # Group the files
    # list "groups" is list of dict, each dict contains actual file list + parser info/config
    groups = []
    # Group by dir overrides other grouping
    if config.get("group_by_dir"):
        groups.append({"files": files,
                       "parsers": [],
                       "params": {}})
    else:
        for format_rules in config.get("known_formats", {}).values():
            format_name_list = format_rules["files"]
            format_groups = {}
            # Check each file for rule matching
            # Match to appropriate group (with same pre/post pattern)
            #   eg a_[match]_b groups with a_[other match]_b but not c_[other match]_d
            for f in files:
                fname = os.path.basename(f).lower().strip()
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
                group_info = {
                    "files": g,
                    "parsers": format_rules["parsers"],
                    "params": format_rules["params"]
                }
                groups.append(group_info)

        # NOTE: Keep this grouping last!
        # Default grouping: Each file is a group
        groups.extend([{"files": [f],
                        "parsers": [],
                        "params": {}}
                       for f in files])

    [groups.extend(group_tree(d, config)) for d in dirs]

    return groups
