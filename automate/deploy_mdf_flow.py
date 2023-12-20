import json
import os
import sys

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


conf_client = globus_sdk.ConfidentialAppAuthClient(
    globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET']
)
cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(conf_client, globus_sdk.FlowsClient.scopes.manage_flows)

flows_client = globus_sdk.FlowsClient(authorizer=cc_authorizer)


globus_auth = GlobusAuthManager(globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])

if len(sys.argv) > 1:
    description = f"MDF Connected deployed from GitHub release {sys.argv[2]}"
    config_file = f"mdf_{sys.argv[1]}_flow_config.json"
    flow_info_file = f'mdf_{sys.argv[1]}_flow_info.json'
else:
    description = "MDF Connect Flow deployed manually"
    config_file = "mdf_flow_config.json"
    flow_info_file = 'mdf_flow_info.json'

# Load other configuration variables
with open(config_file) as f:
    config = json.load(f)

mdf_flow = GlobusAutomateFlow.from_existing_flow(flow_info_file,
                                                 client=flows_client,
                                                 globus_auth=globus_auth)

mdf_flow.update_flow(flow_def=minimus_mdf_flow.flow_def(
    smtp_send_credentials=smtp_send_credentials,
    sender_email=config['sender_email'],
    flow_permissions=config['flow_permissions'],
    description=description,
    administered_by=[
        'urn:globus:groups:id:5fc63928-3752-11e8-9c6f-0e00fd09bf20' # MDF Connect Admins
    ]))

mdf_flow.save_flow(flow_info_file)
print("scope = ", mdf_flow.get_scope_id_for_runAs_role('SubmittingUser')['scopes'][0]['id'])

print("MDF Flow deployed", mdf_flow)
submitting_user_scope_id = mdf_flow.get_scope_id_for_runAs_role('SubmittingUser')['scopes'][0]['id']

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