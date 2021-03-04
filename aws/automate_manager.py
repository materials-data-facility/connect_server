import globus_sdk
from globus_automate_client import FlowsClient
from urllib.parse import urlparse

from globus_sdk import ClientCredentialsAuthorizer, AccessTokenAuthorizer

from globus_automate_flow import GlobusAutomateFlow

globus_secrets = None
mdf_flow = None
tokens = None
MANAGE_FLOWS_SCOPE = "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows"


def authorizer_callback(*args, **kwargs):
    auth = AccessTokenAuthorizer(
        tokens.by_resource_server[mdf_flow.flow_id]['access_token']
    )
    return auth


class AutomateManager:

    def __init__(self, secrets):
        global globus_secrets, mdf_flow, tokens
        globus_secrets = secrets

        self.flow = GlobusAutomateFlow.from_existing_flow("mdf_flow_info.json")
        mdf_flow = self.flow

        conf_client = globus_sdk.ConfidentialAppAuthClient(
            globus_secrets['API_CLIENT_ID'],
            globus_secrets['API_CLIENT_SECRET'])

        requested_scopes = [
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows",
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/view_flows",
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run",
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run_status",
            mdf_flow.flow_scope
        ]

        tokens = conf_client.oauth2_client_credentials_tokens(
            requested_scopes=requested_scopes)

        print("---->", tokens)

        cca = ClientCredentialsAuthorizer(
            conf_client,
            MANAGE_FLOWS_SCOPE,
            tokens.by_resource_server['flows_automated_tests']['access_token'],
            tokens.by_resource_server['flows_automated_tests']['expires_at_seconds']
        )

        self.flows_client = FlowsClient.new_client(
            client_id=globus_secrets['API_CLIENT_ID'],
            authorizer_callback=authorizer_callback,
            authorizer=cca)

        print(self.flows_client)
        self.flow.set_client(self.flows_client)

    def submit(self, mdf_rec, organization):
        destination_parsed = urlparse(organization.data_destinations[0])
        print(destination_parsed)

        assert destination_parsed.scheme == 'globus'

        automate_rec = {
            "mdf_portal_link": "https://example.com/example_link",
            "user_transfer_inputs":
                {
                    "destination_endpoint_id": destination_parsed.netloc,
                    "label": "MDF Flow Test Transfer1",
                    "source_endpoint_id": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
                    "transfer_items": [
                        {
                            "destination_path": destination_parsed.path,
                            "recursive": True,
                            "source_path": "/MDF/mdf_connect/test_files/canonical_datasets/dft/"
                        }
                    ]
                },
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

        print(automate_rec)
        print("Flow is ", self.flow)
        flow_run = self.flow.run_flow(automate_rec)
        print("Result is ", flow_run.action_id)
        print("Status is ", flow_run.get_status())
