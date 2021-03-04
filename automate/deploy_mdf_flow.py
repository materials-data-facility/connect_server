import json

import globus_automate_client

import minimus_mdf_flow  # NOQA
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
                                                flow_permissions=config['flow_permissions']))

mdf_flow.save_flow("mdf_flow_info.json")

print("MDF Flow deployed", mdf_flow)
