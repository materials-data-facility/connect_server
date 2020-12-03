import json
import os
# Do this to get deal with the mdf_connect_server package init assumptions
os.environ["FLASK_ENV"] = 'development'

from minimus_mdf_flow import mdf_flow

from mdf_connect_server.automate.globus_automate_flow import GlobusAutomateFlow, GlobusAutomateFlowDef  # NOQA

automate = GlobusAutomateFlow(native_app_id="417301b1-5101-456a-8a27-423e71a2ae26")


# Schemas of different APs for reference
transfer_input_schema = {
    # "deadline": "datetime str",
    "destination_endpoint_id": "str",
    "label": "str",
    "source_endpoint_id": "str",
    # "sync_level": "str 0-3",
    "transfer_items": [{
        "destination_path": "str",
        "recursive": "bool",
        "source_path": "str"
    }]
}
transfer_permission_schema = {
    "endpoint_id": "string",
    "operation": "string",
    "path": "string",
    "permissions": "string",
    "principal": "string",
    "principal_type": "string"
}
curation_input_schema = {
    "curator_emails": "list of str, or False",
    "curator_template": "str or False",  # variables: $landing_page
    "curation_permissions": "list of str",
    "curation_text": "str or False",
    "author_email": "str or False",
    "author_template": "str or False",  # variables: $curation_task_id, $decision, $reason
    "email_sender": "str",
    "send_credentials": [{}]
}
xtract_input_schema = {
    "metadata_storage_ep": "str",
    "eid": "str",
    "dir_path": "str",
    "mapping": "match",  # ?
    "dataset_mdata": {"test1": "test2"},
    "validator_params": {"schema_branch": "master", "validation_info": {"test1": "test2"}},
    "grouper": "matio"  # options are 'directory/matio'
}


# Load other configuration variables
# Please set these in the configuration file, not in-line here
with open("mdf_flow_config.json") as f:
    config = json.load(f)

mdf_flow_def = GlobusAutomateFlowDef(
    flow_definition=mdf_flow,
    title='The Materials Data Facility Dataset Processing Flow',
    description='Extract, process, and ingest a dataset into MDF Connect.',
    visible_to=config["flow_permissions"],
    runnable_by=config["flow_permissions"],
    administered_by=config["flow_permissions"]
)

# Curation and Transfer Loop subflows (see MDF Utility Flows)
mdf_flow_def["definition"]["States"]["UserTransfer"]["ActionUrl"] = config["transfer_loop_url"]
mdf_flow_def["definition"]["States"]["UserTransfer"]["ActionScope"] = config["transfer_loop_scope"]
mdf_flow_def["definition"]["States"]["DataDestTransfer"]["ActionUrl"] = config["transfer_loop_url"]
mdf_flow_def["definition"]["States"]["DataDestTransfer"]["ActionScope"] = config["transfer_loop_scope"]
mdf_flow_def["definition"]["States"]["CurateSubmission"]["ActionUrl"] = config["curation_subflow_url"]
mdf_flow_def["definition"]["States"]["CurateSubmission"]["ActionScope"] = config["curation_subflow_scope"]
# Config for emails
# admin_email gets notified of critical exceptions in the Flow
mdf_flow_def["definition"]["States"]["ExceptionState"]["Parameters"]["destination"] = config["admin_email"]
# sender_email is the address to send emails with (materialsdatafacility@gmail.com)
mdf_flow_def["definition"]["States"]["ExceptionState"]["Parameters"]["sender"] = config["sender_email"]
mdf_flow_def["definition"]["States"]["NotifyUserEnd"]["Parameters"]["sender"] = config["sender_email"]

# Until Xtract AP is operational, mock the dataset entry output for Xtract AP
mock_dataset_entry = {'dc': {'titles': [{'title': 'Base Deploy Testing Dataset'}],
  'creators': [{'creatorName': 'jgaff',
    'familyName': '',
    'givenName': 'jgaff',
    'affiliations': ['UChicago']}],
  'publisher': 'Materials Data Facility',
  'publicationYear': '2020',
  'resourceType': {'resourceTypeGeneral': 'Dataset',
   'resourceType': 'Dataset'}},
 'mdf': {'source_id': '_test_base_deploy_testing_v5.1',
  'source_name': '_test_base_deploy_testing',
  'version': 5,
  'acl': ['public'],
  'scroll_id': 0,
  'ingest_date': '2020-05-06T17:47:05.219450Z',
  'resource_type': 'dataset'},
 'data': {'endpoint_path': 'globus://e38ee745-6d04-11e5-ba46-22000b92c6ec/MDF/mdf_connect/prod/data/_test_base_deploy_testing_v5.1/',
  'link': 'https://app.globus.org/file-manager?origin_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&origin_path=/MDF/mdf_connect/prod/data/_test_base_deploy_testing_v5.1/',
  'total_size': 4709193},
 'services': {}}

mdf_flow_def["definition"]["States"]["Xtraction"]["Parameters"]["details"] = {
    "output_link": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/test_files/mock_feedstock.json",
    "dataset_entry": mock_dataset_entry
}

automate.deploy_mdf_flow(mdf_flow_def)
automate.save_flow("mdf_flow_info.json")

print("MDF Flow deployed", automate)
