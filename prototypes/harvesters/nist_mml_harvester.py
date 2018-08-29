from werkzeug.datastructures import MultiDict
from mdf_toolbox import MDFConnectClient
from mdf_forge import Forge
from tqdm import tqdm
import requests

base_url = "https://materialsdata.nist.gov/rest/"


def get_all_publications():
    """Generate a list of all items in the NIST MDR repository

    Get only the items that have not been withdrawn

    Returns:
        [int] - Ids of items
    """

    # Initialize the output list
    items = []

    # Get the list of collections
    req = requests.get(base_url + "collections")
    if req.status_code != 200:
        raise _make_failure(req)

    # Get the items from each collection
    collections = req.json()
    collections = [c['id'] for c in collections]

    for collection in collections:

        # Run the request
        req = requests.get(base_url + "collections/{}/items".format(collection))
        if req.status_code != 200:
            raise RuntimeError('Problem connecting with {}. HTTP Status Code: {}'.format(req.url,
                                                                                         req.status_code))
        # Generate the list of items
        my_items = req.json()
        items += [item['id'] for item in my_items if not item['withdrawn'] == 'true']

    return set(items)


def has_been_submitted(item_id, forge):
    """Check if an item has already been submitted to the MDF

    Args:
        item_id (int): Item ID
        forge (Forge): Forge client. Used for querying the MDF Search Index
    Returns:
        (bool) Whether the item has been submitted already
    """

    # Check if the dataset already exists
    results = forge.search('mdf.repositories:=MDR '
                           'AND dc.alternateIdentifiers.alternateIdentifier:={}'.format(item_id),
                           advanced=True)

    # If there is a hit, return True
    return len(results) > 0


def prepare_client_submission(item_id, client):
    """Given an item ID, prepare to submit the MDF client

    Args:
        item_id (int): MDR Item ID
        client (MDFConnectClient): Client, already authenticated
    Returns:
        (MDFConnectClient) Client, primed to call submit
    """

    # Reset the submission
    client.reset_submission()

    # Get the dataset metadata from the MDR
    req = requests.get(base_url + 'items/{}/metadata'.format(item_id))
    if req.status_code != 200:
        raise _make_failure(req)

    # Flatten data into a multidict
    metadata = MultiDict([(x['key'], x['value']) for x in req.json()])

    # Make the DC block
    #  First extract some blocks that may or may not be there
    dc_data = {}

    if 'dc.rights' in metadata:
        dc_data['rightsList'] = [{'rights': metadata['dc.rights']}]
        if 'dc.rights.uri' in metadata:
            dc_data['rightsList'][0]['rightsURI'] = metadata['dc.rights.uri']

    authors = metadata.getlist('dc.contributor.author') if 'dc.contributor.author' in metadata else []
    subjects = [{'subject': s} for s in metadata.get('dc.subject').split(",")] \
        if 'dc.subject' in metadata else []

    #   Make it into the DC block
    client.create_dc_block(title=metadata['dc.title'],
                           authors=authors,
                           affiliations=metadata.getlist('dc.contributor'),
                           publisher='NIST Materials Data Repository',
                           publication_year=int(metadata['dc.date.available'][:4]),
                           resource_type='Dataset',
                           description=metadata.get('dc.abstract', None),
                           subjects=subjects,
                           alternateIdentifiers=[{
                               'alternateIdentifier': metadata['dc.identifier.uri'],
                               'alternateIdentifierType': 'Handle',
                           }, {
                               'alternateIdentifier': str(item_id),
                               'alternateIdentifierType': 'NIST DSpace ID'
                           }],
                           **dc_data)

    # Get the data locations
    req = requests.get(base_url + 'items/{}/bitstreams'.format(item_id))
    if req.status_code != 200:
        raise _make_failure(req)
    for f in req.json():
        client.add_data('{}{}/retrieve'.format("https://materialsdata.nist.gov", f['link']))

    # Add the repository information
    client.add_repositories("MDR")

    # Add in service integrations
    client.add_service("citrine")
    client.add_service("mrr")

    # Make the source name "nist_MDR_[item_number]" to make retrieval easy
    client.set_source_name("mdr_item_{}".format(item_id))
    return client


def _make_failure(req):
    return RuntimeError('Problem connecting with {}. HTTP Status Code: {}'.format(req.url,
                                                                                  req.status_code))


if __name__ == "__main__":
    # Make the client
    client = MDFConnectClient(service_instance="dev")
    forge = Forge('mdf-test')

    # Loop through all items
    for item in tqdm(get_all_publications()):
        # Check if we have done it already
        if has_been_submitted(item, forge):
            continue

        # If not, ready the client to submit
        prepare_client_submission(item, client)

        # Skip if no data
        if len(client.data) == 0:
            continue

        # Turn test on
        client.set_test(True)

        # Submit it
        source_id, success, error = client.submit_dataset()
        if not success:
            print(error)
            raise RuntimeError('Failed for item {}'.format(item))
