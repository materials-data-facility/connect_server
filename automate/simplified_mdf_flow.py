"""Simplified MDF Ingest Flow - v2.

This flow handles file transfer only. Curation is now handled by the MDF server API.

Flow steps:
1. Email admin about new submission
2. Transfer files from user endpoint to MDF repository
3. Notify user of transfer completion

Curation and DOI minting are handled separately via:
- GET  /curation/pending     - List pending submissions
- POST /curation/:id/approve - Approve + mint DOI
- POST /curation/:id/reject  - Reject with reason
"""

import action_providers
from globus_automate_flow import GlobusAutomateFlowDef


def email_submission_to_admin(sender_email, admin_email):
    """Notify admin of new submission."""
    return {
        "EmailSubmission": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/notification/notify",
            "ExceptionOnActionFailure": False,  # Continue even if email fails
            "Parameters": {
                "body_mimetype": "text/html",
                "sender": sender_email,
                "destination": admin_email,
                "subject": "New MDF Dataset Submission",
                "body_template": """
                <html><h1>New Dataset Submitted</h1>
                    <p>A new dataset has been submitted to the Materials Data Facility.</p>
                    <table>
                        <tr><td>Title</td><td>$title</td></tr>
                        <tr><td>Source ID</td><td>$source_id</td></tr>
                        <tr><td>Submitter</td><td>$submitting_user_email</td></tr>
                        <tr><td>Organization</td><td>$organization</td></tr>
                    </table>
                    <p>Review pending submissions at: <a href="$curation_url">$curation_url</a></p>
                </html>
                """,
                "body_variables": {
                    "title.$": "$.dataset_mdata.dc.titles[0].title",
                    "source_id.$": "$.dataset_mdata.mdf.source_id",
                    "submitting_user_email.$": "$.submitting_user_email",
                    "organization.$": "$.dataset_mdata.mdf.organization",
                    "curation_url.$": "$.curation_url",
                },
                "notification_method": "any",
                "notification_priority": "high",
                "send_credentials": [
                    {
                        "credential_method": "email",
                        "credential_type": "ses",
                        "credential_value.$": "$._private_email_credentials",
                    }
                ],
                "__Private_Parameters": ["send_credentials"],
            },
            "ResultPath": "$.EmailSubmissionResult",
            "Next": "CheckMetadataOnly",
        },
    }


def check_metadata_only():
    """Check if this is a metadata-only update (no file transfer needed)."""
    return {
        "CheckMetadataOnly": {
            "Comment": "Skip file transfer if this is a metadata-only update",
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.update_metadata_only",
                    "BooleanEquals": True,
                    "Next": "TransferComplete",
                }
            ],
            "Default": "CreateDatasetDir",
        }
    }


def file_transfer_steps():
    """Transfer files from user endpoint to MDF repository."""
    return {
        "CreateDatasetDir": {
            "Comment": "Create the dataset directory",
            "Type": "Action",
            "ActionUrl": "https://transfer.actions.globus.org/mkdir",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "path.$": "$.user_transfer_inputs.dataset_path",
            },
            "ResultPath": "$.CreateDatasetDirResult",
            "Next": "CreateVersionDir",
        },
        "CreateVersionDir": {
            "Comment": "Create the version subdirectory",
            "Type": "Action",
            "ActionUrl": "https://transfer.actions.globus.org/mkdir",
            "ExceptionOnActionFailure": True,
            "Parameters": {
                "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "path.$": "$.user_transfer_inputs.transfer_items[0].destination_path",
            },
            "ResultPath": "$.CreateVersionDirResult",
            "Catch": [
                {
                    "ErrorEquals": ["ActionFailedException", "States.Runtime", "EndpointError"],
                    "ResultPath": "$.CreateVersionDirResult",
                    "Next": "TransferFailed",
                }
            ],
            "Next": "AddUserPermissions",
        },
        "AddUserPermissions": {
            "Comment": "Temporarily add write permissions for the submitting user",
            "Type": "Action",
            "ActionUrl": "https://transfer.actions.globus.org/manage_permission",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "operation": "CREATE",
                "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "path.$": "$.user_transfer_inputs.transfer_items[0].destination_path",
                "principal_type": "identity",
                "principal.$": "$.user_transfer_inputs.submitting-user-id",
                "permissions": "rw",
            },
            "ResultPath": "$.UserPermissionResult",
            "Catch": [
                {
                    "ErrorEquals": ["ActionFailedException", "States.Runtime", "EndpointError"],
                    "ResultPath": "$.UserPermissionResult",
                    "Next": "TransferFailed",
                }
            ],
            "Next": "ExecuteTransfer",
        },
        "ExecuteTransfer": {
            "Comment": "Transfer data from user endpoint to MDF repository",
            "Type": "Action",
            "ActionUrl": "https://transfer.actions.globus.org/transfer",
            "WaitTime": 86400,  # 24 hours max
            "RunAs": "SubmittingUserV2",
            "Parameters": {
                "source_endpoint.$": "$.user_transfer_inputs.source_endpoint_id",
                "destination_endpoint.$": "$.user_transfer_inputs.destination_endpoint_id",
                "label.$": "$.user_transfer_inputs.label",
                "DATA.$": "$.user_transfer_inputs.transfer_items",
            },
            "ResultPath": "$.TransferResult",
            "Next": "RemoveUserPermissions",
        },
        "RemoveUserPermissions": {
            "Comment": "Remove temporary write permissions",
            "Type": "Action",
            "ActionUrl": "https://transfer.actions.globus.org/manage_permission",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "operation": "DELETE",
                "endpoint_id.$": "$.user_transfer_inputs.destination_endpoint_id",
                "rule_id.$": "$.UserPermissionResult.details.access_id",
            },
            "ResultPath": "$.RemovePermissionResult",
            "Next": "CheckTransferStatus",
        },
        "CheckTransferStatus": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.TransferResult.status",
                    "StringEquals": "SUCCEEDED",
                    "Next": "TransferComplete",
                }
            ],
            "Default": "TransferFailed",
        },
    }


def completion_states(sender_email):
    """Handle transfer completion or failure."""
    return {
        "TransferComplete": {
            "Type": "ExpressionEval",
            "Parameters": {
                "status": "transfer_complete",
                "message.=": "'File transfer complete for ' + `$.dataset_mdata.mdf.source_id` + '. Submission is now pending curation.'",
            },
            "ResultPath": "$.FinalState",
            "Next": "NotifyUserSuccess",
        },
        "TransferFailed": {
            "Type": "ExpressionEval",
            "Parameters": {
                "status": "transfer_failed",
                "message.=": "'File transfer failed for ' + `$.dataset_mdata.mdf.source_id` + '. Please check the flow logs.'",
            },
            "ResultPath": "$.FinalState",
            "Next": "NotifyUserFailure",
        },
        "NotifyUserSuccess": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/notification/notify",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "body_mimetype": "text/html",
                "sender": sender_email,
                "destination.$": "$.submitting_user_email",
                "subject": "MDF Submission - Transfer Complete",
                "body_template": """
                <html>
                <h1>Transfer Complete</h1>
                <p>Your dataset <strong>$source_id</strong> has been transferred to the MDF repository.</p>
                <p>Your submission is now pending curation. You will receive another email when it has been reviewed.</p>
                <p>Thank you for contributing to the Materials Data Facility!</p>
                </html>
                """,
                "body_variables": {
                    "source_id.$": "$.dataset_mdata.mdf.source_id",
                },
                "notification_method": "any",
                "send_credentials": [
                    {
                        "credential_method": "email",
                        "credential_type": "ses",
                        "credential_value.$": "$._private_email_credentials",
                    }
                ],
                "__Private_Parameters": ["send_credentials"],
            },
            "ResultPath": "$.NotifySuccessResult",
            "WaitTime": 300,
            "Next": "EndFlow",
        },
        "NotifyUserFailure": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/notification/notify",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "body_mimetype": "text/html",
                "sender": sender_email,
                "destination.$": "$.submitting_user_email",
                "subject": "MDF Submission - Transfer Failed",
                "body_template": """
                <html>
                <h1>Transfer Failed</h1>
                <p>Your dataset <strong>$source_id</strong> failed to transfer.</p>
                <p>Please check your Globus endpoint permissions and try again.</p>
                <p>View the <a href="https://app.globus.org/runs/$run_id/logs">flow logs</a> for details.</p>
                </html>
                """,
                "body_variables": {
                    "source_id.$": "$.dataset_mdata.mdf.source_id",
                    "run_id.$": "$._context.run_id",
                },
                "notification_method": "any",
                "send_credentials": [
                    {
                        "credential_method": "email",
                        "credential_type": "ses",
                        "credential_value.$": "$._private_email_credentials",
                    }
                ],
                "__Private_Parameters": ["send_credentials"],
            },
            "ResultPath": "$.NotifyFailureResult",
            "WaitTime": 300,
            "Next": "EndFlow",
        },
        "EndFlow": {
            "Type": "Pass",
            "End": True,
        },
    }


def flow_def(
    sender_email,
    admin_email,
    flow_permissions,
    administered_by,
    description="Simplified MDF Ingest Flow - handles file transfer only. Curation via API.",
):
    """Build the simplified flow definition."""
    return GlobusAutomateFlowDef(
        title="MDF Ingest Flow v2 (Simplified)",
        subtitle="Transfer files to MDF repository",
        description=description,
        visible_to=flow_permissions,
        runnable_by=flow_permissions,
        administered_by=administered_by,
        input_schema={},
        flow_definition={
            "StartAt": "EmailSubmission",
            "States": {
                **email_submission_to_admin(sender_email, admin_email),
                **check_metadata_only(),
                **file_transfer_steps(),
                **completion_states(sender_email),
            },
        },
    )


# What was removed from the original flow:
#
# 1. CurateSubmission - Now handled via POST /curation/:id/approve or /reject
# 2. SendCurationEmail - Admin can use the curation dashboard instead
# 3. ChooseAcceptance - Curation decisions are made via API
# 4. FailCuration - Rejection is handled via API
# 5. NeedDOI / MintDOI - DOI minting happens on approval via API
# 6. AddDoiToSearchRecord - DOI is stored in submission record
# 7. SearchIngest - Can be triggered separately after approval
#
# Benefits:
# - Simpler flow with fewer states
# - Curators can use a web dashboard instead of email links
# - DOI minting happens synchronously on approval
# - Better visibility into curation status
# - Easier to add curation workflow features (comments, history, etc.)
