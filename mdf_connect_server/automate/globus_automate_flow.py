import json

import globus_automate_client
import mdf_toolbox

from mdf_connect_server.automate.flow_action import FlowAction


class GlobusAutomateFlow:
    def __init__(self, native_app_id: str, flow_id: str = None, flow_scope: str = None):
        self.flows_client = globus_automate_client.create_flows_client(native_app_id)
        self.flow_id = flow_id
        self.flow_scope = flow_scope

    def __str__(self):
        return f'Globus Automate Flow: id={self.flow_id}, scope={self.flow_scope}'

    def get_runas_auth(self):
        return mdf_toolbox.login(
            services=[self.flow_scope],
            make_clients=False)[self.flow_scope]

    def get_status(self, action_id: str):
        return self.flows_client.flow_action_status(
            self.flow_id,
            self.flow_scope,
            action_id).data

    def get_flow_logs(self, action_id: str):
        return self.flows_client.flow_action_log(
            self.flow_id, self.flow_scope,
            action_id,
            limit=100).data

    def deploy_mdf_flow(self, mdf_flow_def):
        flow_deploy_res = self.flows_client.deploy_flow(
            flow_definition=mdf_flow_def["definition"],
            title=mdf_flow_def["title"],
            description=mdf_flow_def["description"],
            visible_to=mdf_flow_def["visible_to"],
            runnable_by=mdf_flow_def["runnable_by"],
            administered_by=mdf_flow_def["administered_by"],
            # TODO: Make rough schema outline into JSONSchema
            input_schema={},  # mdf_flow_def["schema"],
            validate_definition=True,
            validate_input_schema=True
        )
        self.flow_id = flow_deploy_res["id"]
        self.flow_scope = flow_deploy_res["globus_auth_scope"]

    def run_flow(self, flow_input: dict):
        flow_res = self.flows_client.run_flow(self.flow_id, self.flow_scope, flow_input)
        return FlowAction(self, flow_res.data['action_id'])

    def save_flow(self, path):
        # Save Flow ID/scope for future use
        with open(path, 'w') as f:
            flow_info = {
                "flow_id": self.flow_id,
                "flow_scope": self.flow_scope
            }
            json.dump(flow_info, f)

    def read_flow(self, path):
        # Save Flow ID/scope for future use
        with open(path, 'r') as f:
            flow_info = json.load(f)
            self.flow_id = flow_info['flow_id']
            self.flow_scope = flow_info['flow_scope']
