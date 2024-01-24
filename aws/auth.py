import os

import globus_sdk
import json
from utils import get_secret


def generate_policy(principalId, effect, resource, message="", name=None, identities=[],
                    user_id=None, dependent_token=None, user_email=None,
                    group_info=None):
    if group_info is None:
        group_info = {}
    authResponse = {}
    authResponse["principalId"] = principalId
    if effect and resource:
        policyDocument = {}
        policyDocument["Version"] = "2012-10-17"
        policyDocument["Statement"] = [
            {"Action": "execute-api:Invoke", "Effect": effect, "Resource": resource}
        ]
    authResponse["policyDocument"] = policyDocument
    authResponse["context"] = {
        "name": name,
        "user_id": user_id,
        "identities": str(identities),
        "globus_dependent_token": str(dependent_token),
        "user_email": user_email,
        "message": message,
        "group_info": str(group_info),
    }
    print("AuthResponse", authResponse)
    return authResponse


def lambda_handler(event, context):
    globus_secrets = get_secret(
        secret_name=os.environ["MDF_SECRETS_NAME"],
        region_name=os.environ["MDF_AWS_REGION"],
    )

    # Have to log the event to see why methodArn isn't appearing
    print(json.dumps(event))

    auth_client = globus_sdk.ConfidentialAppAuthClient(
        globus_secrets["API_CLIENT_ID"], globus_secrets["API_CLIENT_SECRET"]
    )

    token = event["headers"]["authorization"].replace("Bearer ", "")

    auth_res = auth_client.oauth2_token_introspect(token, include="identities_set")
    try:
        dependent_token = auth_client.oauth2_get_dependent_tokens(
            token
        ).by_resource_server
        print("Dependent tokens ", dependent_token)

        groups_client = globus_sdk.GroupsClient(authorizer=globus_sdk.AccessTokenAuthorizer(dependent_token['groups.api.globus.org']["access_token"]))
        groups = groups_client.get_my_groups()
        group_info = {group["id"]: {"name": group["name"], "description": group["description"]} for group in groups}
        print("Group info ", group_info)

        if not auth_res:
            return generate_policy(None, "Deny", event["routeArn"],
                                   message="User not found")

        if not auth_res["active"]:
            return generate_policy(None, "Deny", event["routeArn"],
                                   message="User account not active")

        print("auth_res", auth_res)
        user_email = auth_res.get("email", "nobody@nowhere.com")

        return generate_policy(
            auth_res["username"],
            "Allow",
            event["routeArn"],
            name=auth_res["name"],
            identities=auth_res["identities_set"],
            user_id=auth_res["sub"],
            dependent_token=dependent_token,
            user_email=user_email,
            group_info=group_info,
        )
    except:
        return generate_policy(None, "Deny", event["routeArn"],
                               message="Invalid auth token")
