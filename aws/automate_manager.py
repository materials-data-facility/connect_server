import globus_sdk
from globus_automate_client import FlowsClient


class AutomateManager:
    petrel_endpoint_id = "e38ee745-6d04-11e5-ba46-22000b92c6ec"

    def __init__(self, globus_secrets, scope):
        def cli_authorizer_callback(**kwargs):
            conf_client = globus_sdk.ConfidentialAppAuthClient(
                globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])
            auth = globus_sdk.ClientCredentialsAuthorizer(conf_client, scope)
            return auth

        auth = cli_authorizer_callback()

        self.flows_client = FlowsClient.new_client(
            client_id=globus_secrets['API_CLIENT_ID'],
            authorizer_callback=cli_authorizer_callback,
            authorizer=auth)
        print(self.flows_client)

    def submit(self, mdf_rec):
        automate_rec = {
            "mdf_portal_link": "https://example.com/example_link",
            "user_transfer_inputs": [
                {
                    "destination_endpoint_id": self.petrel_endpoint_id,
                    "label": "MDF Flow Test Transfer1",
                    "source_endpoint_id": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
                    "transfer_items": [
                        {
                            "destination_path": "/MDF/mdf_connect/test_files/deleteme/data/test123/",
                            "recursive": True,
                            "source_path": "/MDF/mdf_connect/test_files/canonical_datasets/dft/"
                        }
                    ]
                }
            ],
            "data_destinations": [],
            "data_permissions": {},
            "dataset_acl": [
                "urn:globus:auth:identity:117e8833-68f5-4cb2-afb3-05b25db69be1"
            ],
            "search_index": "aeccc263-f083-45f5-ab1d-08ee702b3384",
            "group_by_dir": True,
            "mdf_storage_ep": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
            "mdf_dataset_path": "/MDF/mdf_connect/test_files/deleteme/data/test123/",
            "dataset_mdata": {},
            "validator_params": {},
            "feedstock_https_domain": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org",
            "curation_input": False,
            "mdf_publish": False,
            "citrine": False,
            "mrr": False
        }
