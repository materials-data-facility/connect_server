import os
from urllib import parse

import globus_sdk
from globus_automate_client import FlowsClient
from urllib.parse import urlparse

from globus_sdk import ClientCredentialsAuthorizer, AccessTokenAuthorizer

from globus_automate_flow import GlobusAutomateFlow

globus_secrets = None
mdf_flow = None
tokens = None
MANAGE_FLOWS_SCOPE = "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows"
test_data_destination = urlparse('globus://e38ee745-6d04-11e5-ba46-22000b92c6ec/MDF/mdf_connect/test_files/deleteme_contents/')

def authorizer_callback(*args, **kwargs):
    auth = AccessTokenAuthorizer(
        tokens.by_resource_server[mdf_flow.flow_id]['access_token']
    )
    return auth


class AutomateManager:

    def __init__(self, secrets):
        # Globals needed for the authorizer_callback
        global tokens, mdf_flow

        tokens = None
        mdf_flow = None

        self.flow = GlobusAutomateFlow.from_existing_flow("mdf_flow_info.json")
        mdf_flow = self.flow

        self.flows_client = None
        self.email_access_key = secrets['SES_ACCESS_KEY']
        self.email_secret = secrets['SES_SECRET']

        self.api_client_id = secrets['API_CLIENT_ID']
        self.api_client_secret = secrets['API_CLIENT_SECRET']

        self.datacite_username = secrets['DATACITE_USERNAME']
        self.datacite_password = secrets['DATACITE_PASSWORD']
        self.datacite_prefix = secrets['DATACITE_PREFIX']

    def authenticate(self):
        global tokens
        conf_client = globus_sdk.ConfidentialAppAuthClient(
            self.api_client_id, self.api_client_secret)

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
            client_id=self.api_client_id,
            authorizer_callback=authorizer_callback,
            authorizer=cca)

        print(self.flows_client)
        self.flow.set_client(self.flows_client)

    def submit(self, mdf_rec, organization,
               submitting_user_token, submitting_user_id, monitor_by_id,
               data_sources, do_curation, is_test=False, update_metadata_only=False):
        # Needs to turn to loop to make as many copies as required by organization
        destination_parsed = urlparse(organization.data_destinations[0])
        assert destination_parsed.scheme == 'globus'

        automate_rec = {
            "mdf_portal_link": "https://example.com/example_link",
            "user_transfer_inputs": self.create_transfer_items(
                data_sources=data_sources,
                organization=organization,
                submitting_user_id=submitting_user_id,
                test_submit=is_test
            ),
            "data_destinations": [],
            "data_permissions": {},
            "dataset_acl": [
                "urn:globus:auth:identity:117e8833-68f5-4cb2-afb3-05b25db69be1"
            ],
            "search_index": "ab71134d-0b36-473d-aa7e-7b19b2124c88",
            # @Ben group_by_dir This will be an XTract flow option
            "group_by_dir": True,
            "mdf_storage_ep": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
            "mdf_dataset_path": "/MDF/mdf_connect/test_files/deleteme/data/test123/",
            "dataset_mdata": mdf_rec,
            "validator_params": {},
            # Maybe not needed?
            "feedstock_https_domain": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org",

            "curation_input": do_curation,
            #@Ben Change to something like get_doi: True
            "mdf_publish": False,
            # @Ben this will default to False.
            "citrine": False,
            # @Ben this will default to True as long as the data are public. That will be in a separate flow
            "mrr": False,
            "update_metadata_only": update_metadata_only,
            "mint_doi": organization.mint_doi,

            # Is this actually used?
            "path": "/~/<username>/<data-directory>",
            "admin_email": "bengal1@illinois.edu",
            "_private_email_credentials": {
                "aws_access_key_id": self.email_access_key,
                "aws_secret_access_key": self.email_secret,
                "region_name": "us-east-1"
            },
            "_datacite_username": self.datacite_username,
            "_datacite_password": self.datacite_password,
            "datacite_prefix": self.datacite_prefix,
            "datacite_as_test": is_test,
            "_tokens": {
                'SubmittingUser': submitting_user_token['access_token']
            }
        }
        print("Flow is ", self.flow)
        print("Automate_rec is ", automate_rec)
        flow_run = self.flow.run_flow(automate_rec,
                                      monitor_by=monitor_by_id,
                                      label=f'MDF Submission {mdf_rec["mdf"]["source_id"]}')
        print("Result is ", flow_run.action_id)
        print("Status is ", flow_run.get_status())
        return flow_run.action_id

    def create_transfer_items(self, data_sources, organization,
                              submitting_user_id, test_submit=False):

        destination_parsed = urlparse(organization.data_destinations[0]) \
            if not test_submit  else test_data_destination

        user_transfer_inputs = {"destination_endpoint_id": destination_parsed.netloc,
                                "label": "MDF Flow Test Transfer1",
                                "source_endpoint_id": None,
                                "submitting-user-id": submitting_user_id,
                                "transfer_items": []
                                }

        for data_source_url in data_sources:
            transfer_params = parse.parse_qs(parse.urlparse(data_source_url).query)

            # @TODO
            # This should also handle the google drive URL mapping (See utils.py:67)
            # Standardize the URL since user's could have created a link from the
            # left (origin) or right (destination) side of the Globus File browser
            # We want standard origin terminology
            if "destination_id" in transfer_params:
                transfer_params["origin_id"] = transfer_params['destination_id']

            if "destination_path" in transfer_params:
                transfer_params['origin_path'] = transfer_params['destination_path']

            if "origin_id" in transfer_params and "origin_path" in transfer_params:
                if not user_transfer_inputs["source_endpoint_id"]:
                    user_transfer_inputs["source_endpoint_id"] = transfer_params['origin_id'][0]
                else:
                    if user_transfer_inputs["source_endpoint_id"] != transfer_params['origin_id'][0]:
                        raise ValueError(
                            "All datasets must come from the same globus endpoint")
                user_transfer_inputs['transfer_items'].append(
                    {
                        "destination_path": destination_parsed.path,
                        "recursive": True,
                        "source_path": transfer_params['origin_path'][0]
                    }
                )
            else:
                raise ValueError("Globus destination URI must include endpoint ID and path")
        return user_transfer_inputs

    def get_status(self, action_id: str):
        return self.flow.get_status(action_id)

    def get_log(self, action_id: str):
        return self.flow.get_flow_logs(action_id)
