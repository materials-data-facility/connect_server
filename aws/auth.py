import globus_sdk
import boto3
import json


def get_secret():
    secret_name = "Globus"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()

    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    return eval(get_secret_value_response['SecretString'])


def generate_policy(principalId, effect, resource, message="", name=None, identities=[],
                    user_id=None, dependent_token=None, user_email=None):
    authResponse = {}
    authResponse['principalId'] = principalId
    if effect and resource:
        policyDocument = {}
        policyDocument['Version'] = '2012-10-17'
        policyDocument['Statement'] = [
            {'Action': 'execute-api:Invoke',
             'Effect': effect,
             'Resource': resource
             }
        ]
    authResponse['policyDocument'] = policyDocument
    authResponse['context'] = {
        'name': name,
        'user_id': user_id,
        'identities': str(identities),
        'globus_dependent_token': str(dependent_token),
        'user_email': user_email
    }
    print("AuthResponse", authResponse)
    return authResponse


def lambda_handler(event, context):
    globus_secrets = get_secret()

    #Have to log the event to see why methodArn isn't appearing
    print(json.dumps(event));

    auth_client = globus_sdk.ConfidentialAppAuthClient(
        globus_secrets['API_CLIENT_ID'], globus_secrets['API_CLIENT_SECRET'])

    token = event['headers']['authorization'].replace("Bearer ", "")

    auth_res = auth_client.oauth2_token_introspect(token, include="identities_set")
    try:
        dependent_token = auth_client.oauth2_get_dependent_tokens(token).by_resource_server
        print("Dependent token ", dependent_token)

        if not auth_res:
            return generate_policy(None, 'Deny', event['routeArn'],
                                   message='User not found')

        if not auth_res['active']:
            return generate_policy(None, 'Deny', event['routeArn'],
                                   message='User account not active')

        print("auth_res", auth_res)
        user_email = auth_res.get("email", "nobody@nowhere.com")

        return generate_policy(auth_res['username'], 'Allow', event['routeArn'],
                               name=auth_res["name"],
                               identities=auth_res["identities_set"],
                               user_id=auth_res['sub'],
                               dependent_token=dependent_token,
                               user_email=user_email)
    except:
        return generate_policy(None, 'Deny', event['routeArn'],
                               message='Invalid auth token')
