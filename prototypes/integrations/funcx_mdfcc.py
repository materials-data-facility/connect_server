def mdf_submit(local_json_path, globus_uri, test=True, with_doi=True, update=False):
    """Submit dataset to MDF Connect.

    Arguments:
        local_json_path (str): The path to the local JSON metadata file.
        globus_uri (str): The URI to the Globus Endpoint and path, in the form:
                "globus://[endpoint id]/[path to data directory]"
        test (bool): Is this a test submission (test submissions generate test DOIs
                and populate the Search index "mdf-test" instead of "mdf")?
                Default True.
        with_doi (bool): Should a DOI be minted? (Includes test DOI.)
                Default True.
        update (bool): Has this submission been made before? If so, an update will be made
                to the previous submission. Test submissions and non-test submissions
                are separate.
    """
    import json
    import os
    from mdf_connect_client import MDFConnectClient

    mapping = {
        "custom.dynamic_mean_window_size": "xpcs.dynamic_mean_window_size",
        "custom.lld": "xpcs.lld",
        "custom.sigma": "xpcs.sigma",
        "custom.snophi": "xpcs.snophi",
        "custom.snoq": "xpcs.snoq"
    }
    mdfcc = MDFConnectClient()
    with open(local_json_path) as f:
        md = json.load(f)
    # DC block (title, authors, publisher, pub year, subjects)
    mdfcc.create_dc_block(title=os.path.basename(local_json_path).replace(".json", ""),
                          authors=[creator["creatorName"] for creator in md["creators"]],
                          publisher=md.get("publisher"),
                          publication_year=md.get("publication_year"),
                          subjects=[subject["subject"] for subject in md.get("subjects", [])])
    # Add data
    mdfcc.add_data_source(globus_uri)
    # Add JSON mapping
    mdfcc.add_index("json", mapping)
    # Set test flag
    mdfcc.set_test(test)
    # Add XPCS as organization
    mdfcc.add_organization("XPCS 8-ID")
    # Set group-by-dir flag
    mdfcc.set_conversion_config({"group_by_dir": True})
    # Add MDF Publish service
    if with_doi:
        mdfcc.add_service("mdf_publish")

    # Submit dataset
    sub_res = mdfcc.submit_dataset(update=update)
    if not sub_res["success"]:
        raise RuntimeError(sub_res["error"])
    else:
        print("Submission '{}' started".format(sub_res["source_id"]))
        return "Submission '{}' started".format(sub_res["source_id"])


def mdf_check_status(source_id):
    """Check status of MDF Connect submission.

    Arguments:
        source_id (str): The source_id of the dataset to check. The source_id is returned
                from the submission call.
    """
    from mdf_connect_client import MDFConnectClient

    mdfcc = MDFConnectClient()
    status = mdfcc.check_status(source_id, raw=True)
    if not status["success"]:
        raise RuntimeError(status["error"])
    else:
        print(status["status"]["status_message"])
        return status["status"]["status_message"]
