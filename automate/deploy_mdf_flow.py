import json
import os
# Do this to deal with the mdf_connect_server package init assumptions
os.environ["FLASK_ENV"] = 'development'

import globus_automate_client  # NOQA
import transfer_loop_flow  # NOQA
import minimus_mdf_flow  # NOQA

from mdf_connect_server.automate.globus_automate_flow import GlobusAutomateFlow  # NOQA

native_app_id = "417301b1-5101-456a-8a27-423e71a2ae26"
client = globus_automate_client.create_flows_client(native_app_id)

with open(".mdfsecrets", 'r') as f:
    secrets = json.load(f)
    smtp_send_credentials = [{
        "credential_type": "smtp",
        "credential_value": {
            "hostname": secrets['smtp_hostname'],
            "username": secrets['smtp_user'],
            "password": secrets['smtp_pass']
        }
    }]

# Load other configuration variables
# Please set these in the configuration file, not in-line here
with open("mdf_flow_config.json") as f:
    config = json.load(f)

transfer_loop_subflow = \
    GlobusAutomateFlow.from_flow_def(client,
                                     flow_def=transfer_loop_flow.flow_def(config["flow_permissions"]))

mdf_flow = GlobusAutomateFlow.from_flow_def(client,
                                            flow_def=minimus_mdf_flow.flow_def(
                                                smtp_send_credentials=smtp_send_credentials,
                                                sender_email=config['sender_email'],
                                                flow_permissions=config[
                                                    'flow_permissions'],
                                                transfer_loop_subflow=transfer_loop_subflow))

mdf_flow.save_flow("mdf_flow_info.json")

print("MDF Flow deployed", mdf_flow)
