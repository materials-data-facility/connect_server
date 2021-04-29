import re
import urllib
from urllib import parse

import boto3

GLOBUS_LINK_FORMS = [
    "^https:\/\/www\.globus\.org\/app\/transfer",
    # noqa: W605 (invalid escape char '\/')
    "^https:\/\/app\.globus\.org\/file-manager",  # noqa: W605
    "^https:\/\/app\.globus\.org\/transfer",  # noqa: W605
    "^https:\/\/.*globus.*(?=.*origin_id)(?=.*origin_path)",  # noqa: W605
    "^https:\/\/.*globus.*(?=.*destination_id)(?=.*destination_path)"  # noqa: W605
]


def normalize_globus_uri(location):
    """Normalize a Globus Web App link or Google Drive URI into a globus:// URI.
    For Google Drive URIs, the file(s) must be shared with
    materialsdatafacility@gmail.com.
    If the URI is not a Globus Web App link or Google Drive URI,
    it is returned unchanged.

    Arguments:
        location (str): One URI to normalize.

    Returns:
        str: The normalized URI, or the original URI if no normalization was possible.
    """
    loc_info = urllib.parse.urlparse(location)
    # Globus Web App link into globus:// form
    if any([re.search(pattern, location) for pattern in GLOBUS_LINK_FORMS]):
        data_info = urllib.parse.unquote(loc_info.query)
        # EP ID is in origin or dest
        ep_start = data_info.find("origin_id=")
        if ep_start < 0:
            ep_start = data_info.find("destination_id=")
            if ep_start < 0:
                raise ValueError("Invalid Globus Transfer UI link")
            else:
                ep_start += len("destination_id=")
        else:
            ep_start += len("origin_id=")
        ep_end = data_info.find("&", ep_start)
        if ep_end < 0:
            ep_end = len(data_info)
        ep_id = data_info[ep_start:ep_end]

        # Same for path
        path_start = data_info.find("origin_path=")
        if path_start < 0:
            path_start = data_info.find("destination_path=")
            if path_start < 0:
                raise ValueError("Invalid Globus Transfer UI link")
            else:
                path_start += len("destination_path=")
        else:
            path_start += len("origin_path=")
        path_end = data_info.find("&", path_start)
        if path_end < 0:
            path_end = len(data_info)
        path = data_info[path_start:path_end]

        # Make new location
        new_location = "globus://{}{}".format(ep_id, path)

    # Google Drive protocol into globus:// form
    elif loc_info.scheme in ["gdrive", "google", "googledrive"]:
        # Correct form is "google:///path/file.dat"
        # (three slashes - two for scheme end, one for path start)
        # But if a user uses two slashes, the netloc will incorrectly be the top dir
        # (netloc="path", path="/file.dat")
        # Otherwise netloc is nothing (which is correct)
        if loc_info.netloc:
            gpath = "/" + loc_info.netloc + loc_info.path
        else:
            gpath = loc_info.path
        # Don't use os.path.join because gpath starts with /
        # GDRIVE_ROOT does not end in / to make compatible
        new_location = "globus://{}{}{}".format(config["GDRIVE_EP"], config["GDRIVE_ROOT"], gpath)

    # Default - do nothing
    else:
        new_location = location

    return new_location


def get_secret():
    secret_name = "Globus"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()

    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    return eval(get_secret_value_response['SecretString'])
