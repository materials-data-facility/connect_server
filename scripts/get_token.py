import json
import os
import sys

import globus_sdk

if "API_CLIENT_ID" in os.environ:
    globus_secrets = {
        "API_CLIENT_ID": os.environ["API_CLIENT_ID"],
        "API_CLIENT_SECRET": os.environ["API_CLIENT_SECRET"],
        "smtp_hostname": os.environ["SMTP_HOSTNAME"],
        "smtp_user": os.environ["SMTP_USER"],
        "smtp_pass": os.environ["SMTP_PASS"]
    }
else:
    if len(sys.argv) == 2:
        secret_file = f"../automate/.mdfsecrets.{sys.argv[1]}"
    else:
        secret_file = "../automate/.mdfsecretsCCC"

    with open(secret_file, 'r') as f:
        globus_secrets = json.load(f)

client = globus_sdk.ConfidentialAppAuthClient(globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])
scopes = "urn:globus:auth:scope:groups.api.globus.org:all openid email profile " "urn:globus:auth:scope:transfer.api.globus.org:all"
token_response = client.oauth2_client_credentials_tokens(requested_scopes=scopes)
print(token_response)
tokens = token_response.by_resource_server["groups.api.globus.org"]
GROUP_TOKEN = tokens["access_token"]
print("Token ",GROUP_TOKEN)
print(tokens)