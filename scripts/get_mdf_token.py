import sys

from mdf_connect_client import MDFConnectClient
"""
This script authenticates the user against the MDF Application and prints out the 
bearer token that can be used in REST calls
"""
if len(sys.argv) != 2:
    print("Usage: python get_mdf_token.py <MDF Application URL>")
    sys.exit(1)

mdfcc = MDFConnectClient(service_instance=sys.argv[1])
mdfcc.logout()
mdfcc = MDFConnectClient(service_instance=sys.argv[1])
print(mdfcc._MDFConnectClient__authorizer.access_token)
