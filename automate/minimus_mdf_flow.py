import action_providers
from globus_automate_flow import GlobusAutomateFlowDef


def flow_def(smtp_send_credentials, sender_email, flow_permissions):
    return GlobusAutomateFlowDef(
        title="Transfer Loop Flow",
        description="Perform multiple Globus Transfers",
        visible_to=flow_permissions,
        runnable_by=flow_permissions,
        administered_by=flow_permissions,

        flow_definition={
            "StartAt": "StartSubmission",
            "States": {
                "StartSubmission": {
                    "Type": "Pass",
                    "Next": "UserPermissions"
                },
                "Check Metadata Only":{
                    "Comemnt": "Checks whether flow just updates the metadata",
                    "Type": "Choice",
                    "Choices": [
                        {
                        "Variable": "$.update_meta_only",
                        "BooleanEquals": True,
                        "Next": "ChooseCuration "
                        }
                    ],
                    "Default": "UserPermissions"
                }
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

                "ChooseCuration": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.curation_input",
                            "BooleanEquals": False,
                            "Next": "SearchIngest"
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
                        "sender": "materialsdatafacility@uchicago.edu",
                        "destination": "bengal1@illinois.edu",
                        "subject": "Materials Data Facility Curation Request",
                        "body_template": "Please either Approve or Deny the secure egress request here: $landing_page_url",
                        "body_variables": {
                            "landing_page_url.=": "'https://actions.globus.org/weboption/landing_page/' + `$._context.action_id`"
                        },                        "notification_method": "any",
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
                            "preamble_text.=": "'A new dataset has been submitted. ' + `$.mdf_portal_link` +' Please review it to allow processing to continue.'"                        },
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
                            "Next": "SearchIngest"
                        },
                        {
                            "Variable": "$.CurateResult.details.name",
                            "StringEquals": "rejected",
                            "Next": "FailCuration"
                        }
                    ],
                    "Default": "ExceptionState"
                },
                "SearchIngest":{
                    "Type": "Action",
                    "ActionUrl": "https://actions.globus.org/search/ingest",
                    "ExceptionOnActionFailure": True,
                    "ResultPath": "$.SearchIngestResult",
                    "Parameters": {
                        "search_index.$": "$.search_index",
                        "subject.$": "$.dataset_mdata.mdf.source_id",
                        "visible_to": [
                            "public"
                        ],
                        "content.$": "$.dataset_mdata"
                    },
                    "Next": "SubmissionSuccess"
                },
                "SubmissionSuccess": {
                    "Type": "ExpressionEval",
                    "Parameters": {
                        "title": "Submission Ingested Successfully",
                        "message.=": "'Submission Flow succeeded. Your submission (' + `$.dataset_mdata.mdf.source_id`+ ') can be viewed at this link: ' + `$.mdf_portal_link`"
                    },
                    "ResultPath": "$.FinalState",
                    "Next": "ChooseNotifyUserEnd"
                },
                "FailUserTransfer": {
                    "Type": "ExpressionEval",
                    "Parameters": {
                        "title": "MDF Submission Failed",
                        "message.=": "'Your MDF submission ' + `$.source_id` + ' failed to transfer to MDF:\n' + `$.UserTransferResult.details`"
                    },
                    "ResultPath": "$.FinalState",
                    "Next": "ChooseNotifyUserEnd"
                },
                "FailUserPermission": {
                    "Type": "ExpressionEval",
                    "Parameters": {
                        "title": "MDF Permission Settings Failed",
                        "message.=": "'Your MDF submission ' + `$.source_id` + ' failed to transfer to MDF:\n' + `$.UserPermissionResult.details`"
                    },
                    "ResultPath": "$.FinalState",
                    "Next": "ChooseNotifyUserEnd"
                },
                "FailCuration": {
                    "Type": "ExpressionEval",
                    "Parameters": {
                        "title": "MDF Submission Rejected",
                        "message.=": "'Your submission (' + `$.source_id` + ') was rejected by a curator and did not complete the ingestion process. The curator gave the following reason for rejection: '+ `$.CurateResult.details.output.CurationResult.details.parameters.user_input`"
                    },
                    "ResultPath": "$.FinalState",
                    "Next": "ChooseNotifyUserEnd"
                },
                "ExceptionState": {
                    "Type": "Action",
                    "ActionUrl": "https://actions.globus.org/notification/notify",
                    "ExceptionOnActionFailure": True,
                    "Parameters": {
                        "body_mimetype": "text/html",
                        "sender": "materialsdatafacility@uchicago.edu",
                        "destination": "bengal1@illinois.edu",
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
                    "Next": "ChooseNotifyUserEnd"
                },
                "ChooseNotifyUserEnd": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.curation_input",
                            "BooleanEquals": False,
                            "Next": "EndSubmission"
                        }
                    ],
                    "Default": "NotifyUserEnd"
                },
                "NotifyUserEnd": {
                    "Type": "Action",
                    "ActionUrl": action_providers.notify,
                    "ExceptionOnActionFailure": True,
                    "Parameters": {
                        "body_template.$": "$.FinalState.message",
                        "destination.$": "$.curation_input.author_email",
                        "send_credentials": smtp_send_credentials,
                        "__Private_Parameters": [
                            "send_credentials"
                        ],
                        "sender": sender_email,
                        "subject.$": "$.FinalState.title"
                    },
                    "ResultPath": "$.NotifyUserResult",
                    "WaitTime": 86400,
                    "Next": "EndSubmission"
                },
                "EndSubmission": {
                    "Type": "Pass",
                    "End": True
                }
            }
        })
