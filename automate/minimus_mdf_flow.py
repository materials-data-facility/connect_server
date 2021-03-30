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
                "UserPermissions": {
                    "Comment": "Temporarily add write permissions for the submitting user",
                    "Type": "Action",
                    "ActionUrl": "https://actions.globus.org/transfer/set_permission",
                    "Parameters": {
                        "operation": "CREATE",
                        "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                        "path.$": "$.user_transfer_inputs.transfer_items[0].destination_path",
                        "principal_type": "identity",
                        "principal.$": "$.user_transfer_inputs.submitting-user-id",
                        "permissions": "rw"
                    },
                    "ResultPath": "$.UserPermissionResult",
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
                    "Type": "Pass",
                    "Parameters": {},
                    "ResultPath": "$.UndoUserPermissionResult",
                    "Next": "CheckUserTransfer"
                },
                "CheckUserTransfer": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.UserTransferResult.status",
                            "StringEquals": "SUCCEEDED",
                            "Next": "ChooseNotifyUserEnd"  # Was Xtract
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
