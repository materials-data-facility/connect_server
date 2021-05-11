import os
from urllib import parse

import globus_sdk
from globus_automate_client import FlowsClient
from urllib.parse import urlparse

from globus_sdk import ClientCredentialsAuthorizer, AccessTokenAuthorizer

from globus_automate_flow import GlobusAutomateFlow
from utils import normalize_globus_uri

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
            tokens.by_resource_server['flows.globus.org']['access_token'],
            tokens.by_resource_server['flows.globus.org']['expires_at_seconds']
        )

        self.flows_client = FlowsClient.new_client(
            client_id=globus_secrets['API_CLIENT_ID'],
            authorizer_callback=authorizer_callback,
            authorizer=cca)

        print(self.flows_client)
        self.flow.set_client(self.flows_client)

        self.email_access_key = globus_secrets['SES_ACCESS_KEY']
        self.email_secret = globus_secrets['SES_SECRET']

    def submit(self, mdf_rec, organization,
               submitting_user_token, submitting_user_id,
               data_sources, do_curation):
        destination_parsed = urlparse(organization.data_destinations[0])
        assert destination_parsed.scheme == 'globus'

        automate_rec = {
            "mdf_portal_link": "https://example.com/example_link",
            "user_transfer_inputs": self.create_transfer_items(
                data_sources=data_sources,
                organization=organization,
                submitting_user_id=submitting_user_id
            ),
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

    def create_transfer_items(self, data_sources, organization, submitting_user_id):
        destination_parsed = urlparse(organization.data_destinations[0])

        user_transfer_inputs = {"destination_endpoint_id": destination_parsed.netloc,
                                "label": "MDF Flow Test Transfer1",
                                "source_endpoint_id": None,
                                "submitting-user-id": submitting_user_id,
                                "transfer_items": []
                                }

        for data_source_url in data_sources:
            transfer_params = parse.parse_qs(parse.urlparse(data_source_url).query)
            if "destination_id" in transfer_params and "destination_path" in transfer_params:
                if not user_transfer_inputs["source_endpoint_id"]:
                    user_transfer_inputs["source_endpoint_id"] = transfer_params['destination_id'][0]
                else:
                    if user_transfer_inputs["source_endpoint_id"] != transfer_params['destination_id'][0]:
                        raise ValueError(
                            "All datasets must come from the same globus endpoint")
                user_transfer_inputs['transfer_items'].append(
                    {
                        "destination_path": destination_parsed.path,
                        "recursive": True,
                        "source_path": transfer_params['destination_path'][0]
                    }
                )
            else:
                raise ValueError("Globus destination URI must include endpoint ID and path")
        return user_transfer_inputs

    def get_status(self, action_id: str):
        return self.flow.get_status(action_id)

    def get_log(self, action_id: str):
        return self.flow.get_flow_logs(action_id)
