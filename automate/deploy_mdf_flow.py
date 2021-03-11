import json
import sys

import globus_automate_client

import minimus_mdf_flow  # NOQA
from globus_auth_manager import GlobusAuthManager
from globus_automate_flow import GlobusAutomateFlow  # NOQA

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


native_app_id = "417301b1-5101-456a-8a27-423e71a2ae26"  # Premade native app ID
flows_client = globus_automate_client.create_flows_client(native_app_id)
globus_auth = GlobusAuthManager(globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])

# Load other configuration variables
# Please set these in the configuration file, not in-line here
with open("mdf_flow_config.json") as f:
    config = json.load(f)

flow_def = minimus_mdf_flow.flow_def(smtp_send_credentials=smtp_send_credentials,
                                     sender_email=config['sender_email'],
                                     flow_permissions=config['flow_permissions'])
print(flow_def.flow_definition)

mdf_flow = GlobusAutomateFlow.from_flow_def(flows_client,
                                            flow_def=minimus_mdf_flow.flow_def(
                                                smtp_send_credentials=smtp_send_credentials,
                                                sender_email=config['sender_email'],
                                                flow_permissions=config['flow_permissions']),
                                            globus_auth=globus_auth)

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