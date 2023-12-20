from mdf_connect_client import MDFConnectClient
mdfcc = MDFConnectClient(service_instance="dev")

print(mdfcc.check_all_submissions(newer_than_date=(2023, 10, 1)))