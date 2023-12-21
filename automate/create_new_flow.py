import json
import sys

import globus_sdk

import minimus_mdf_flow
from globus_auth_manager import GlobusAuthManager
from globus_automate_flow import GlobusAutomateFlow, GlobusAutomateFlowDef

if len(sys.argv) > 1:
    secrets_file = f".mdfsecrets.{sys.argv[1]}"
else:
    secrets_file = ".mdfsecrets"

with open(secrets_file, 'r') as f:
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

with open("mdf_flow_config.json") as f:
    config = json.load(f)

description = "MDF Connect Flow deployed manually"
globus_auth = GlobusAuthManager(globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])
flow_def=minimus_mdf_flow.flow_def(
    smtp_send_credentials=smtp_send_credentials,
    sender_email=config['sender_email'],
    flow_permissions=config['flow_permissions'],
    description=description,
    administered_by=[
        'urn:globus:groups:id:5fc63928-3752-11e8-9c6f-0e00fd09bf20' # MDF Connect Admins
    ])

flow_def.title = "Production MDF Ingest Flow"
mdf_flow = GlobusAutomateFlow.from_flow_def(client=flows_client,
                                            flow_def=flow_def,
                                            globus_auth=globus_auth)

print(mdf_flow)

mdf_flow.save_flow("mdf_flow_info.prod.json")

print("MDF Flow deployed", mdf_flow)
submitting_user_scope_id = mdf_flow.get_scope_id_for_runAs_role('SubmittingUser')['scopes'][0]['id']
print(f"RunAs Dependent scope ID = {submitting_user_scope_id}")

submitting_user_scope_uri = mdf_flow.get_scope_uri_for_runAs_role('SubmittingUser')
print(f"RunAs Dependent Scope URI (will appear in the dict of dependent scopes in the authorizer) = {submitting_user_scope_uri}")

print("PUT this to https://auth.globus.org/v2/api/scopes/:SCOPE_ID where SCOPE_ID is the ID of your API's scope")
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