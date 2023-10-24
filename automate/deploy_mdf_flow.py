import json
import os
import sys

import globus_automate_client
import globus_sdk

import minimus_mdf_flow  # NOQA
from globus_auth_manager import GlobusAuthManager
from globus_automate_flow import GlobusAutomateFlow  # NOQA

if "API_CLIENT_ID" in os.environ:
    globus_secrets = {
        "API_CLIENT_ID": os.environ["API_CLIENT_ID"],
        "API_CLIENT_SECRET": os.environ["API_CLIENT_SECRET"],
        "smtp_hostname": os.environ["SMTP_HOSTNAME"],
        "smtp_user": os.environ["SMTP_USER"],
        "smtp_pass": os.environ["SMTP_PASS"]
    }
else:
    with open(".mdfsecrets", 'r') as f:
        globus_secrets = json.load(f)

smtp_send_credentials = [{
    "credential_type": "smtp",
    "credential_value": {
        "hostname": globus_secrets['smtp_hostname'],
        "username": globus_secrets['smtp_user'],
        "password": globus_secrets['smtp_pass']
    }
}]


# Load other configuration variables
# Please set these in the configuration file, not in-line here
with open("mdf_flow_config.json") as f:
    config = json.load(f)

native_app_id = "e6128bac-8f6a-4b19-adf8-716ed9c4d56c"  # MDF Automate Client app ID

conf_client = globus_sdk.ConfidentialAppAuthClient(
    globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET']
)
cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(conf_client, globus_sdk.FlowsClient.scopes.manage_flows)

flows_client = globus_sdk.FlowsClient(authorizer=cc_authorizer)


globus_auth = GlobusAuthManager(globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])

# Load other configuration variables
# Please set these in the configuration file, not in-line here
with open("mdf_flow_config.json") as f:
    config = json.load(f)

flow_def = minimus_mdf_flow.flow_def(smtp_send_credentials=smtp_send_credentials,
                                     sender_email=config['sender_email'],
                                     flow_permissions=config['flow_permissions'],
                                     administered_by=[
                                         'urn:globus:auth:identity:2400d618-4c18-479d-b8bf-32b7497cc673'
                                         # Ethan
                                     ])
print(flow_def.flow_definition)
mdf_flow = GlobusAutomateFlow.from_existing_flow("mdf_flow_info.json",
                                                 client=flows_client,
                                                 globus_auth=globus_auth)
mdf_flow.update_flow(flow_def=minimus_mdf_flow.flow_def(
    smtp_send_credentials=smtp_send_credentials,
    sender_email=config['sender_email'],
    flow_permissions=config['flow_permissions'],
    administered_by=[
        'urn:globus:auth:identity:2400d618-4c18-479d-b8bf-32b7497cc673'  # Ethan
    ]))

mdf_flow.save_flow("mdf_flow_info.json")
print("scope = ", mdf_flow.get_scope_for_runAs_role('SubmittingUser')['scopes'][0]['id'])

print("MDF Flow deployed", mdf_flow)
submitting_user_scope_id = mdf_flow.get_scope_for_runAs_role('SubmittingUser')['scopes'][0]['id']

connect_scope_def = {
    "scope": {
        "name": "MDF Connect",
        "description": "Submit data to MDF Connect",
        "scope_suffix": "connect",
        "dependent_scopes": [
            {
                "scope": "80fa5a88-ae26-4db7-be3a-c5f4cf4ac8d2",
                "optional": False,
                "requires_refresh_token": False
            },
            {
                "scope": "0b21a92f-2fed-4b2d-a481-50a58cc796b9",
                "optional": False,
                "requires_refresh_token": True
            },
             {
                    "optional": False,
                    "requires_refresh_token": True,
                    "scope": submitting_user_scope_id
            }
        ]
    }
}

print(json.dumps(connect_scope_def))