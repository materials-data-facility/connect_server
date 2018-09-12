from ctypes import c_bool
import json
import logging
import multiprocessing
import os
from queue import Empty

from mdf_connect_server.config import CONVERTER_CONF
from mdf_connect_server.processor import transform

NUM_TRANSFORMERS = 5

GROUPING_RULES = CONVERTER_CONF["grouping_rules"]
REPOSITORY_RULES = CONVERTER_CONF["repository_rules"]

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

    # Process dataset entry
    # Fetch custom block descriptors
    new_custom = {}
    # __custom block descriptors
    # Turn _description => _desc
    for key, val in convert_params["dataset"].pop("__custom", {}).items():
        if key.endswith("_description"):
            new_custom[key[:-len("ription")]] = val
        else:
            new_custom[key] = val
    for key, val in convert_params["dataset"].pop("__custom_desc", {}).items():
        if key.endswith("_desc"):
            new_custom[key] = val
        elif key.endswith("_description"):
            new_custom[key[:-len("ription")]] = val
        else:
            new_custom[key+"_desc"] = val
    if new_custom:
        convert_params["dataset"]["__custom"] = new_custom

    full_dataset = convert_params["dataset"]
    if full_dataset.get("mdf", {}).get("repositories"):
        full_dataset["mdf"]["repositories"] = list(expand_repository_tags(
                                                    full_dataset["mdf"]["repositories"]))

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


def expand_repository_tags(input_tags, repo_rules=REPOSITORY_RULES):
    # Remove duplicates
    input_tags = set(input_tags)
    # Tags in final form
    final_tags = set()
    # Tags requiring expansion
    parent_tags = set()

    for tag in input_tags:
        # If tag in in canonical form
        if tag in repo_rules.keys():
            # Add canonical tag and aliases to final_tags
            final_tags.add(tag)
            final_tags.update(repo_rules[tag].get("aliases", []))
            # Add parents' canonical forms to processing list
            parent_tags.update(repo_rules[tag].get("parent_tags", []))
        # tag is not in canonical form
        else:
            # Find canonical form of tag, add to processing list, remove tag from input list
            for name, info in repo_rules.items():
                if tag in info["aliases"]:
                    parent_tags.add(name)

    # Process tags requiring expansion
    # Recursion ends when no parents are left
    if parent_tags:
        final_tags.update(expand_repository_tags(parent_tags))

    return final_tags
