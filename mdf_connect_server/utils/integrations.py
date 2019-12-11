# TODO: Can FuncX functions call each other like this?
# TODO: How to keep DataCite credentials secret on FuncX?

def citrine_upload(citrine_data, api_key, mdf_dataset, previous_id=None, public=True):
    import os
    from citrination_client import CitrinationClient

    cit_client = CitrinationClient(api_key).data
    source_id = mdf_dataset.get("mdf", {}).get("source_id", "NO_ID")
    try:
        cit_title = mdf_dataset["dc"]["titles"][0]["title"]
    except (KeyError, IndexError, TypeError):
        cit_title = "Untitled"
    try:
        cit_desc = " ".join([desc["description"]
                             for desc in mdf_dataset["dc"]["descriptions"]])
        if not cit_desc:
            raise KeyError
    except (KeyError, IndexError, TypeError):
        cit_desc = None

    # Create new version if dataset previously created
    if previous_id:
        try:
            rev_res = cit_client.create_dataset_version(previous_id)
            assert rev_res.number > 1
        except Exception:
            previous_id = "INVALID"
        else:
            cit_ds_id = previous_id
            cit_client.update_dataset(cit_ds_id,
                                      name=cit_title,
                                      description=cit_desc,
                                      public=False)
    # Create new dataset if not created
    if not previous_id or previous_id == "INVALID":
        try:
            cit_ds_id = cit_client.create_dataset(name=cit_title,
                                                  description=cit_desc,
                                                  public=False).id
            assert cit_ds_id > 0
        except Exception as e:
            print("{}: Citrine dataset creation failed: {}".format(source_id, repr(e)))
            if previous_id == "INVALID":
                return {
                    "success": False,
                    "error": "Unable to create revision or new dataset in Citrine"
                }
            else:
                return {
                    "success": False,
                    "error": "Unable to create Citrine dataset, possibly due to duplicate entry"
                }

    success = 0
    failed = 0
    for path, _, files in os.walk(os.path.abspath(citrine_data)):
        for pif in files:
            up_res = cit_client.upload(cit_ds_id, os.path.join(path, pif))
            if up_res.successful():
                success += 1
            else:
                print("{}: Citrine upload failure: {}".format(source_id, str(up_res)))
                failed += 1

    cit_client.update_dataset(cit_ds_id, public=public)

    return {
        "success": bool(success),
        "cit_ds_id": cit_ds_id,
        "success_count": success,
        "failure_count": failed
        }


def datacite_mint_doi(dc_md, test, url=None, doi=None):
    import json
    import requests

    if not doi and not dc_md.get("identifier") and not dc_md.get("identifiers"):
        doi = make_dc_doi(test)

    doi_md = translate_dc_schema(dc_md, doi=doi, url=url)
    creds = get_dc_creds(test)
    res = requests.post(creds["DC_URL"], auth=(creds["DC_USERNAME"], creds["DC_PASSWORD"]),
                        json=doi_md)
    try:
        res_json = res.json()
    except json.JSONDecodeError:
        return {
            "success": False,
            "error": "DOI minting failed",
            "details": res.content
        }

    if res.status_code >= 300:
        return {
            "success": False,
            "error": "; ".join([err["title"] for err in res_json["errors"]])
        }
    else:
        return {
            "success": True,
            # "datacite_full": res_json,
            # "dataset": doi_md,
            "datacite": res_json["data"]
        }


def datacite_update_doi(doi, updates, test, url=None):
    import json
    import requests

    update_md = translate_dc_schema(updates, doi=doi, url=url)
    creds = get_dc_creds(test)
    res = requests.put(creds["DC_URL"]+doi, auth=(creds["DC_USERNAME"], creds["DC_PASSWORD"]),
                       json=update_md)
    try:
        res_json = res.json()
    except json.JSONDecodeError:
        return {
            "success": False,
            "error": "DOI update failed",
            "details": res.content
        }

    if res.status_code >= 300:
        return {
            "success": False,
            "error": "; ".join([err["title"] for err in res_json["errors"]])
        }
    else:
        return {
            "success": True,
            "datacite": res_json["data"]
        }


def get_dc_creds(test):
    # TODO
    raise NotImplementedError
    '''
    if test:
        return CONFIG["DATACITE_CREDS"]["TEST"]
    else:
        return CONFIG["DATACITE_CREDS"]["NONTEST"]
    '''


def make_dc_doi(test, num_sections, num_chars):
    """Create a random (but unused) DOI.

    Arguments:
        test (bool): Generate a test DOI?
        num_sections (int): Number of sections of random characters to add.
                (ex. 10.123/xxxx-xxxx-xxxx is three sections of four characters)
        num_chars (int): Number of characters per section.

    Returns:
        str: The generated DOI.
    """
    import random
    import string
    import requests

    creds = get_dc_creds(test)
    doi_unique = False
    while not doi_unique:
        # Create new DOI by appending random characters to prefix
        new_doi = creds["DC_PREFIX"]
        for i in range(num_sections):
            new_doi += "".join(random.choices(string.ascii_lowercase + string.digits,
                                              k=num_chars))
            new_doi += "-"
        new_doi = new_doi.strip("-")

        # Check that new_doi is unique, not used previously
        # NOTE: Technically there is a non-zero chance that two identical IDs are generated
        #       before either submit to DataCite.
        #       However, the probability is low enough that we do not mitigate this
        #       condition. Should it occur, the later submission will fail.
        doi_fetch = requests.get(creds["DC_URL"]+new_doi)
        if doi_fetch.status_code == 404:
            doi_unique = True
    return new_doi


def translate_dc_schema(dc_md, doi=None, url=None):
    """Translate Datacite Schema to Datacite DOI Schema (slightly different)."""
    from copy import deepcopy

    doi_data = deepcopy(dc_md)

    # url
    if url:
        doi_data["url"] = url

    # identifiers
    if doi_data.get("identifier"):
        doi_data["doi"] = doi_data["identifier"]["identifier"]
        doi_data["identifiers"] = [doi_data.pop("identifier")]
    elif doi:
        doi_data["doi"] = doi
        doi_data["identifiers"] = [{
            "identifier": doi,
            "identifierType": "DOI"
        }]

    # creators
    if doi_data.get("creators"):
        new_creators = []
        for creator in doi_data["creators"]:
            if creator.get("creatorName"):
                creator["name"] = creator.pop("creatorName")
            if creator.get("affiliations"):
                creator["affiliation"] = creator.pop("affiliations")
            new_creators.append(creator)
        doi_data["creators"] = new_creators

    # contributors
    if doi_data.get("contributors"):
        new_contributors = []
        for contributor in doi_data["contributors"]:
            if contributor.get("contributorName"):
                contributor["name"] = contributor.pop("contributorName")
            if contributor.get("affiliations"):
                contributor["affiliation"] = contributor.pop("affiliations")
            new_contributors.append(contributor)
        doi_data["contributors"] = new_contributors

    # types
    if doi_data.get("resourceType"):
        doi_data["types"] = doi_data.pop("resourceType")

    # alternateIdentifiers (does not exist)
    if doi_data.get("alternateIdentifiers"):
        doi_data.pop("alternateIdentifiers")

    doi_data["event"] = "publish"
    doi_md = {
        "data": {
            "type": "dois",
            "attributes": doi_data
        }
    }

    return doi_md
