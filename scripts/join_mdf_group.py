import globus_sdk
CLIENT_ID = 'c296acd8-f454-4ad7-b582-51a48685e775'
CLIENT_SECRET = 'IgkdmTz8cE+7hS/uo8JPmXE7xtsIJ1xg+2AC2AlHF5g='
client = globus_sdk.ConfidentialAppAuthClient(CLIENT_ID, CLIENT_SECRET)
scopes = "urn:globus:auth:scope:groups.api.globus.org:all openid email profile " "urn:globus:auth:scope:transfer.api.globus.org:all"
token_response = client.oauth2_client_credentials_tokens(requested_scopes=scopes)
tokens = token_response.by_resource_server["groups.api.globus.org"]
GROUP_TOKEN = tokens["access_token"]
print("Token ",GROUP_TOKEN)
a = globus_sdk.AccessTokenAuthorizer(GROUP_TOKEN)
groups_client = globus_sdk.GroupsClient(authorizer=a)
batch = globus_sdk.BatchMembershipActions()
batch.join("c296acd8-f454-4ad7-b582-51a48685e775")
groups_client.batch_membership_action("cc192dca-3751-11e8-90c1-0a7c735d220a", batch)
print(groups_client.get_my_groups())

