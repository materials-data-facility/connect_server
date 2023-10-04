import json
from typing import Mapping, Any, Optional, List

from globus_automate_client import FlowsClient

import mdf_toolbox

from flow_action import FlowAction
from globus_auth_manager import GlobusAuthManager


class GlobusAutomateFlowDef:
    def __init__(self,
                 flow_definition: Mapping[str, Any],
                 title: str,
                 subtitle: Optional[str] = None,
                 description: Optional[str] = None,
                 keywords: List[str] = [],
                 visible_to: List[str] = [],
                 runnable_by: List[str] = [],
                 administered_by: List[str] = [],
                 input_schema: Optional[Mapping[str, Any]] = None):
        self.flow_definition = flow_definition
        self.title = title
        self.subtitle = subtitle
        self.description = description
        self.keywords = keywords
        self.visible_to = visible_to
        self.runnable_by = runnable_by
        self.administered_by = administered_by
        self.input_schema = input_schema


class GlobusAutomateFlow:
    def __init__(self, client: FlowsClient, globus_auth: GlobusAuthManager = None):
        self.flows_client = client
        self.flow_id = None
        self.flow_scope = None
        self.saved_flow = None
        self.runAsScopes = None
        self.globus_auth = globus_auth

    @classmethod
    def from_flow_def(cls, client: FlowsClient,
                      flow_def: GlobusAutomateFlowDef,
                      globus_auth: GlobusAuthManager = None):
        result = GlobusAutomateFlow(client, globus_auth)
        result._deploy_mdf_flow(flow_def)
        return result

    @classmethod
    def from_existing_flow(cls, path: str = None,
                           flow_id: str = None,
                           flow_scope: str = None,
                           client: FlowsClient = None,
                           globus_auth: GlobusAuthManager = None):
        """
        Create a GlobusAutomateFlow object from an existing flow. The flow-id and
        flow-scope can either come out of a json file, or be provided directly.
        """
        if path is None:
            assert flow_id is not None and flow_scope is not None
        else:
            assert path is None

        result = GlobusAutomateFlow(client, globus_auth)
        if path:
            result.read_flow(path)
        else:
            result.flow_id = flow_id
            result.flow_scope = flow_scope
        return result

    def set_client(self, client):
        self.flows_client = client

    @property
    def url(self):
        return "https://flows.globus.org/flows/" + self.flow_id

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

    def update_flow(self, flow_def: GlobusAutomateFlowDef):
        flow_deploy_res = self.flows_client.update_flow(
            flow_id=self.flow_id,
            flow_definition=flow_def.flow_definition,
            title=flow_def.title,
            subtitle=flow_def.subtitle,
            description=flow_def.description,
            visible_to=flow_def.visible_to,
            runnable_by=flow_def.runnable_by,
            administered_by=flow_def.administered_by,
            # TODO: Make rough schema outline into JSONSchema
            input_schema=flow_def.input_schema,
            validate_definition=True
        )
        self.flow_id = flow_deploy_res["id"]
        self.flow_scope = flow_deploy_res["globus_auth_scope"]
        self.saved_flow = self.flows_client.get_flow(self.flow_id).data
        self.runAsScopes = self.saved_flow['globus_auth_scopes_by_RunAs']
        print(self.runAsScopes)

    def _deploy_mdf_flow(self, mdf_flow_def: GlobusAutomateFlowDef):
        flow_deploy_res = self.flows_client.deploy_flow(
            flow_definition=mdf_flow_def.flow_definition,
            title=mdf_flow_def.title,
            subtitle=mdf_flow_def.subtitle,
            description=mdf_flow_def.description,
            visible_to=mdf_flow_def.visible_to,
            runnable_by=mdf_flow_def.runnable_by,
            administered_by=mdf_flow_def.administered_by,
            # TODO: Make rough schema outline into JSONSchema
            input_schema=mdf_flow_def.input_schema,
            validate_definition=True
        )
        self.flow_id = flow_deploy_res["id"]
        self.flow_scope = flow_deploy_res["globus_auth_scope"]
        self.saved_flow = self.flows_client.get_flow(self.flow_id).data
        self.runAsScopes = self.saved_flow['globus_auth_scopes_by_RunAs']
        print(self.runAsScopes)

    def run_flow(self, flow_input: dict, monitor_by: list = None, label=None):
        flow_res = self.flows_client.run_flow(self.flow_id, self.flow_scope,
                                              flow_input, monitor_by=monitor_by,
                                              label=label)
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

    def get_scope_for_runAs_role(self, rolename):
        print("--->RunAsScopes ", self.runAsScopes[rolename])
        return self.globus_auth.scope_id_from_uri(self.runAsScopes[rolename])
