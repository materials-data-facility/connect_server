import action_providers
from globus_automate_flow import GlobusAutomateFlowDef


def check_update_metadata_only():
    return{
        "Check Metadata Only": {
            "Comment": "Checks whether flow just updates the metadata",
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.update_metadata_only",
                    "BooleanEquals": True,
                    "Next": "ChooseCuration"
                }
            ],
            "Default": "CreateDestinationDir"
        }
    }


def file_transfer_steps():
    """
    Steps to transfer user data to MDF repository:
        * Add a temporary write permission to the repo so we can execute the transfer
          as the submitting user
        * Execute the transfer using runAs
        * Remove the temporary write permission
    """
    return {
        "CreateDestinationDir": {
            "Comment": "Create a destination directory for the transfered data",
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/transfer/mkdir",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "path.$": "$.user_transfer_inputs.transfer_items[0].destination_path",
            },
            "ResultPath": "$.CreateDestinationDirResult",
            "Catch": [
                {
                    "ErrorEquals": ["ActionFailedException", "States.Runtime"],
                    "Next": "FailUserPermission"
                }
            ],

            "Next": "UserPermissions"
        },


        "UserPermissions": {
            "Comment": "Temporarily add write permissions for the submitting user",
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/transfer/set_permission",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "operation": "CREATE",
                "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "path.$": "$.user_transfer_inputs.transfer_items[0].destination_path",
                "principal_type": "identity",
                "principal.$": "$.user_transfer_inputs.submitting-user-id",
                "permissions": "rw"
            },
            "ResultPath": "$.UserPermissionResult",
            "Catch": [
                {
                    "ErrorEquals": ["ActionFailedException", "States.Runtime"],
                    "Next": "FailUserPermission"
                }
            ],

            "Next": "UserTransfer"
        },
        "UserTransfer": {
            "Comment": "Copy from user's endpoint to organization's dataset destination",
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/transfer/transfer",
            "WaitTime": 86400,
            "RunAs": "SubmittingUser",
            "Parameters": {
                "source_endpoint_id.$": "$.user_transfer_inputs.source_endpoint_id",
                "destination_endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "label.$": "$.user_transfer_inputs.label",
                "transfer_items.$": "$.user_transfer_inputs.transfer_items"
            },
            "ResultPath": "$.UserTransferResult",
            "Next": "UndoUserPermissions"
        },
        "UndoUserPermissions": {
            "Comment": "Remove temporary write permissions for the submitting user",
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/transfer/set_permission",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "operation": "DELETE",
                "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "rule_id.$": "$.UserPermissionResult.details.access_id"
            },
            "ResultPath": "$.UndoUserPermissionResult",
            "Catch": [
                {
                    "ErrorEquals": ["ActionFailedException", "States.Runtime"],
                    "Next": "FailUserPermission"
                }
            ],
            "Next": "CheckUserTransfer"
        },
        "CheckUserTransfer": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.UserTransferResult.status",
                    "StringEquals": "SUCCEEDED",
                    "Next": "ChooseCuration"
                }
            ],
            "Default": "FailUserTransfer"
        },
        "FailUserTransfer": {
            "Type": "ExpressionEval",
            "Parameters": {
                "title": "MDF Submission Failed",
                "message.=": "'Your MDF submission ' + `$.source_id` + ' failed to transfer to MDF:\n' + `$.UserTransferResult.details`"
            },
            "ResultPath": "$.FinalState",
            "Next": "NotifyUserEnd"
        },
        "FailUserPermission": {
            "Type": "ExpressionEval",
            "Parameters": {
                "title": "MDF Permission Settings Failed",
                "message.=": "'Your MDF submission ' + `$.source_id` + ' failed to transfer to MDF:\n' + `$.UserPermissionResult.details`"
            },
            "ResultPath": "$.FinalState",
            "Next": "NotifyUserEnd"
        },
    }


def curation_steps(sender_email):
    """
    Steps for allowing an administrator to curate submissions:
        * Check to see if curation has been requested
        * Format and send an email to the administrator to curate the submission
        * Wait for administrator response
        * Respond to administrator acceptance or rejection
    """
    return {
        "ChooseCuration": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.curation_input",
                    "BooleanEquals": False,
                    "Next": "NeedDOI"
                }
            ],
            "Default": "SendCurationEmail"
        },
        "SendCurationEmail": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/notification/notify",
            "ExceptionOnActionFailure": True,
            "ResultPath": "$.CurationEmailResult",
            "Parameters": {
                "body_mimetype": "text/html",
                "sender": sender_email,
                "destination": "blaiszik@uchicago.edu",
                # "destination": "materialsdatafacility@uchicago.edu",
                "subject": "Materials Data Facility Curation Request",
                "body_template": "Please either Approve or Deny the secure egress request here: $landing_page_url",
                "body_variables": {
                    "landing_page_url.=": "'https://actions.globus.org/weboption/landing_page/' + `$._context.action_id`"
                }, "notification_method": "any",
                "notification_priority": "high",
                "send_credentials": [
                    {
                        "credential_method": "email",
                        "credential_type": "ses",
                        "credential_value.$": "$._private_email_credentials"
                    }
                ],
                "__Private_Parameters": [
                    "send_credentials"
                ]
            },
            "Next": "CurateSubmission"
        },
        "CurateSubmission": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/weboption/wait_for_option",
            "ExceptionOnActionFailure": True,
            "ResultPath": "$.CurateResult",
            "Parameters": {
                "landing_page": {
                    "url_suffix.$": "$._context.action_id",
                    "header_background": "#FFF8C6",
                    "header_icon_url": "https://materialsdatafacility.org/images/MDF-logo@2x.png",
                    "header_icon_link": "https://materialsdatafacility.org",
                    "header_text": "Curate an MDF Dataset",
                    "page_title": "Materials Data Facility",
                    "preamble_text.=": "'A new dataset has been submitted. ' + `$.mdf_portal_link` +' Please review it to allow processing to continue.'"},
                "options": [
                    {
                        "name": "accepted",
                        "description": "Accept dataset",
                        "url_suffix.=": "`$._context.action_id` + '_approve'",
                        "completed_message": "Indexing of dataset submission will commence"
                    },
                    {
                        "name": "rejected",
                        "description": "Reject Dataset",
                        "url_suffix.=": "`$._context.action_id` + '_deny'",
                        "completed_message": "Submission has been cancelled"
                    }
                ]
            },
            "WaitTime": 86400,
            "Next": "ChooseAcceptance"
        },
        "ChooseAcceptance": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.CurateResult.details.name",
                    "StringEquals": "accepted",
                    "Next": "NeedDOI"
                },
                {
                    "Variable": "$.CurateResult.details.name",
                    "StringEquals": "rejected",
                    "Next": "FailCuration"
                }
            ],
            "Default": "ExceptionState"
        },

        "FailCuration": {
            "Type": "ExpressionEval",
            "Parameters": {
                "title": "MDF Submission Rejected",
                "message.=": "'Your submission (' + `$.source_id` + ') was rejected by a curator and did not complete the ingestion process. The curator gave the following reason for rejection: '+ `$.CurateResult.details.output.CurationResult.details.parameters.user_input`"
            },
            "ResultPath": "$.FinalState",
            "Next": "NotifyUserEnd"
        },

    }


def mint_doi_steps():
    """
    Mint a DOI for the dataset if requested
        * Check to see if mint doi requested
        * Submit to DataCite action provider
        * Merge the resulting DOI into the search record
    """
    return {
        "NeedDOI": {
            "Comment": "Checks whether flow needs to mint a DOI",
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.mint_doi",
                    "BooleanEquals": True,
                    "Next": "MintDOI"
                }
            ],
            "Default": "SearchIngest"
        },
        "MintDOI": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/datacite/mint/basic_auth",
            "ExceptionOnActionFailure": True,
            "ResultPath": "$.DoiResult",
            "Parameters": {
                "as_test.$": "$.datacite_as_test",
                "username.$": "$._private_datacite_credentials._datacite_username",
                "password.$": "$._private_datacite_credentials._datacite_password",
                "Doi": {
                    "id.$": "$.datacite_prefix",
                    "type": "dois",
                    "attributes": {
                        "prefix.$": "$.datacite_prefix",
                        "creators.$": "$.dataset_mdata.dc.creators",
                        "titles.$": "$.dataset_mdata.dc.titles",
                        "publisher.$": "$.dataset_mdata.dc.publisher",
                        "publicationYear.$": "$.dataset_mdata.dc.publicationYear",
                        "url.$": "$.mdf_portal_link"
                    }
                },
                "__Private_Parameters": [
                    "username",
                    "password"
                ]
            },
            "Next": "AddDoiToSearchRecord"
        },
        "AddDoiToSearchRecord": {
            "Type": "ExpressionEval",
            "ResultPath": "$.dataset_mdata.dc.identifier",
            "Parameters": {
                "identifierType": "DOI",
                "identifier.$": "$.DoiResult.details.data.attributes.doi"
            },
            "Next": "SearchIngest",
        },
    }


def search_ingest_steps():
    return {
        "SearchIngest": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/search/ingest",
            "Comment": "Submit dataset_mdata to Search",
            "ExceptionOnActionFailure": True,
            "ResultPath": "$.SearchIngestResult",
            "Parameters": {
                "search_index.$": "$.search_index",
                "subject.$": "$.dataset_mdata.mdf.versioned_source_id",
                "visible_to": [
                    "public"
                ],
                "content.$": "$.dataset_mdata"
            },
            "Next": "SubmissionSuccess"
        },
    }


def notify_user_steps(sender_email):
    """
    Check on the final status of the submission and notify the submitting user as
    appropriate
    """
    return {
        "SubmissionSuccess": {
            "Type": "ExpressionEval",
            "Parameters": {
                "title": "Submission Ingested Successfully",
                "message.=": "'Submission Flow succeeded. Your submission (' + `$.dataset_mdata.mdf.source_id`+ ') can be viewed at this link: ' + `$.mdf_portal_link`"
            },
            "ResultPath": "$.FinalState",
            "Next": "NotifyUserEnd"
        },
        "NotifyUserEnd": {
            "Type": "Action",
            "ActionUrl": action_providers.notify,
            "ExceptionOnActionFailure": True,
            "Parameters": {
                "body_template.$": "$.FinalState.message",
                "destination.$": "$.submitting_user_email",
                "sender": sender_email,
                "subject.$": "$.FinalState.title",
                "send_credentials": [
                    {
                        "credential_method": "email",
                        "credential_type": "ses",
                        "credential_value.$": "$._private_email_credentials"
                    }
                ],
                "__Private_Parameters": [
                    "send_credentials"
                ]
            },
            "ResultPath": "$.NotifyUserResult",
            "WaitTime": 86400,
            "Next": "EndSubmission"
        },
    }


def exception_state(sender_email):
    """
    Handle any general exceptions that occur as a result of the flow
    """
    return{
        "ExceptionState": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/notification/notify",
            "ExceptionOnActionFailure": True,
            "Parameters": {
                "body_mimetype": "text/html",
                "sender": sender_email,
                "destination.$": "$.submitting_user_email",
                "subject": "Submission Failed to Ingest",
                "body_template.=": "'Submission ' + `$.source_id` + ' fatally errored processing in Flow '+ `$._context.action_id` + '. Please review the Flow log for details about this exception.'",

                "notification_method": "any",
                "notification_priority": "high",
                "send_credentials": [
                    {
                        "credential_method": "email",
                        "credential_type": "ses",
                        "credential_value.$": "$._private_email_credentials"
                    }
                ],
                "__Private_Parameters": [
                    "send_credentials"
                ]
            },
            "ResultPath": "$.ExceptionNotifyResult",
            "WaitTime": 86400,
            "Next": "NotifyUserEnd"
        },
    }


def flow_def(smtp_send_credentials, sender_email, flow_permissions, administered_by):
    return GlobusAutomateFlowDef(
        title="MDF Ingest Flow",
        description="Ingest Materials Data Facility Submissions",
        visible_to=flow_permissions,
        runnable_by=flow_permissions,
        administered_by=administered_by,
        input_schema={},

        flow_definition={
            "StartAt": "StartSubmission",
            "States": {
                "StartSubmission": {
                    "Type": "Pass",
                    "Next": "Check Metadata Only"
                },
                **check_update_metadata_only(),
                **file_transfer_steps(),
                **curation_steps(sender_email),
                **mint_doi_steps(),
                **search_ingest_steps(),
                **notify_user_steps(sender_email),
                **exception_state(sender_email),

                "EndSubmission": {
                    "Type": "Pass",
                    "End": True
                }
            }
        })
