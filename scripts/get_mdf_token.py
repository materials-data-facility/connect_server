from mdf_connect_client import MDFConnectClient
"""
This script authenticates the user against the MDF Application and prints out the 
bearer token that can be used in REST calls
"""
mdfcc = MDFConnectClient(service_instance="prod")
print(mdfcc._MDFConnectClient__authorizer.access_token)
