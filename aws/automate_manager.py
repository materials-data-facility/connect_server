import globus_sdk
from globus_automate_client import FlowsClient


class AutomateManager:
    def __init__(self, globus_secrets, scope):
        def cli_authorizer_callback(**kwargs):
            conf_client = globus_sdk.ConfidentialAppAuthClient(
                globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])
            auth = globus_sdk.ClientCredentialsAuthorizer(conf_client, scope)
            return auth

        auth = cli_authorizer_callback()

        self.flows_client = FlowsClient.new_client(
            client_id=globus_secrets['API_CLIENT_ID'],
            authorizer_callback=cli_authorizer_callback,
            authorizer=auth)
        print(self.flows_client)
