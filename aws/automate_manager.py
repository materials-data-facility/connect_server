import os

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

        self.email_access_key = globus_secrets['SES_ACCESS_KEY']
        self.email_secret = globus_secrets['SES_SECRET']

    def submit(self, mdf_rec, organization, submitting_user_token, submitting_user_id):
        destination_parsed = urlparse(organization.data_destinations[0])
        print(destination_parsed)

        assert destination_parsed.scheme == 'globus'

        do_curation = mdf_rec.get("curation")

        automate_rec = {
            "mdf_portal_link": "https://example.com/example_link",
            "user_transfer_inputs":
                {
                    "destination_endpoint_id": destination_parsed.netloc,
                    "label": "MDF Flow Test Transfer1",
                    "source_endpoint_id": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
                    "submitting-user-id": submitting_user_id,
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
            "search_index": "ab71134d-0b36-473d-aa7e-7b19b2124c88",
            "group_by_dir": True,
            "mdf_storage_ep": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
            "mdf_dataset_path": "/MDF/mdf_connect/test_files/deleteme/data/test123/",
            "dataset_mdata": mdf_rec,
            "validator_params": {},
            "feedstock_https_domain": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org",
            "curation_input": do_curation,
            "mdf_publish": False,
            "citrine": False,
            "mrr": False,
            "path": "/~/<username>/<data-directory>",
            "admin_email": "bengal1@illinois.edu",
            "_private_email_credentials": {
                "aws_access_key_id": self.email_access_key,
                "aws_secret_access_key": self.email_secret,
                "region_name": "us-east-1"
            },
            "_tokens": {
                'SubmittingUser': submitting_user_token['access_token']
            }
        }

        print(automate_rec)
        print("Flow is ", self.flow)
        flow_run = self.flow.run_flow(automate_rec, monitor_by=[submitting_user_id])
        print("Result is ", flow_run.action_id)
        print("Status is ", flow_run.get_status())
        return flow_run.action_id

    def get_status(self, action_id: str):
        return self.flow.get_status(action_id)

    def get_log(self, action_id: str):
        return self.flow.get_flow_logs(action_id)
