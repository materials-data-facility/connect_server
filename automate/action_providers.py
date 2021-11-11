transfer = "https://actions.globus.org/transfer/transfer"
transfer_scope = "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer"

expression_eval = "https://actions.globus.org/expression_eval"
expression_eval_scope = "https://auth.globus.org/scopes/5fac2e64-c734-4e6b-90ea-ff12ddbf9653/expression"

notify = "https://actions.globus.org/notification/notify"

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