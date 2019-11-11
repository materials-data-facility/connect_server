def mdf_submit():
    """Submit dataset to MDF Connect."""
    import json
    import os
    from mdf_connect_client import MDFConnectClient

    # TODO: Fill in or generate
    path_to_json = ("/home/jgaff/Downloads/A002_SMB_A_BR_SAMPLE_noExtension_att0_Lq0"
                    "_001_0001-0256.json")  # Local path to metadata file
    globus_uri = "globus://endpoint-id/path/to/data/dir"  # Globus-accessible uri to data
    test_submission = True  # Is this a test submission?
    publish_with_doi = True  # Should a DOI be minted? (Includes test DOI)
    is_resubmission = False  # Has this submission been made before?

    mapping = {}
    mdfcc = MDFConnectClient()
    with open(path_to_json) as f:
        md = json.load(f)
    # DC block (title, authors, publisher, pub year, subjects)
    mdfcc.create_dc_block(title=os.path.basename(path_to_json).replace(".json", ""),
                          authors=[creator["creatorName"] for creator in md["creators"]],
                          publisher=md.get("publisher"),
                          publication_year=md.get("publication_year"),
                          subjects=[subject["subject"] for subject in md.get("subjects", [])])
    # Add data
    mdfcc.add_data_source(globus_uri)
    # Add JSON mapping
    mdfcc.add_index("json", mapping)
    # Set test flag
    mdfcc.set_test(test_submission)
    # Add Argonne as organization
    mdfcc.add_organization("ANL")
    # Add MDF Publish service
    if publish_with_doi:
        mdfcc.add_service("mdf_publish")

    # Submit dataset
    sub_res = mdfcc.submit_dataset(update=is_resubmission)
    if not sub_res["success"]:
        raise RuntimeError(sub_res["error"])
    else:
        print("Submission '{}' started".format(sub_res["source_id"]))
        return "Submission '{}' started".format(sub_res["source_id"])


def mdf_check_status():
    """Check status of MDF Connect submission."""
    from mdf_connect_client import MDFConnectClient

    # TODO: Find source_id dynamically
    source_id = "_test_narayanan_a002_smb_00010256_v1.1"

    mdfcc = MDFConnectClient()
    status = mdfcc.check_status(source_id, raw=True)
    if not status["success"]:
        raise RuntimeError(status["error"])
    else:
        print(status["status"]["status_message"])
        return status["status"]["status_message"]
