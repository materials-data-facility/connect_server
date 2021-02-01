
# DynamoDB setup
import json
import logging
import os

import boto3
import jsonschema
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger(__name__)

class DynamoManager:
    DMO_SCHEMA = {
        # "TableName": DMO_TABLE,
        "AttributeDefinitions": [{
            "AttributeName": "source_id",
            "AttributeType": "S"
        }],
        "KeySchema": [{
            "AttributeName": "source_id",
            "KeyType": "HASH"
        }],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 20,
            "WriteCapacityUnits": 20
        }
    }
    STATUS_STEPS = (
        ("sub_start", "Submission initialization"),
        ("old_cancel", "Cancellation of previous submissions"),
        ("data_download", "Connect data download"),
        ("data_transfer", "Data transfer to primary destination"),
        ("extracting", "Metadata extraction"),
        ("curation", "Dataset curation"),
        ("ingest_search", "MDF Search ingestion"),
        ("ingest_backup", "Data transfer to secondary destinations"),
        ("ingest_publish", "MDF Publish publication"),
        ("ingest_citrine", "Citrine upload"),
        ("ingest_mrr", "Materials Resource Registration"),
        ("ingest_cleanup", "Post-processing cleanup")
    )

    def __init__(self, config):
        self.dmo_client = boto3.resource('dynamodb',
                                         aws_access_key_id=config["AWS_KEY"],
                                         aws_secret_access_key=config["AWS_SECRET"],
                                         region_name="us-east-1")
        self.dmo_tables = {
            "status": config["DYNAMO_STATUS_TABLE"],
            "curation": config["DYNAMO_CURATION_TABLE"]
        }

        # Load status schema
        with open(os.path.join(config["SCHEMA_PATH"],
                               "internal_status.json")) as schema_file:
            self.schema = json.load(schema_file)

        self.resolver = jsonschema.RefResolver(
            base_uri="file://{}/".format(config["SCHEMA_PATH"]),
            referrer=self.schema)

    def old_get_dmo_table(self, table_name):
        # For compatibility with legacy utils in this file
        try:
            table_key = self.dmo_tables[table_name]
        except KeyError:
            return {
                "success": False,
                "error": "Invalid table '{}'".format(table_name)
            }
        try:
            table = self.dmo_client.Table(table_key)
            dmo_status = table.table_status
            if dmo_status != "ACTIVE":
                raise ValueError("Table not active")
        except (ValueError, boto3.client.meta.client.exceptions.ResourceNotFoundException):
            return {
                "success": False,
                "error": "Table does not exist or is not active"
                }
        except Exception as e:
            return {
                "success": False,
                "error": repr(e)
                }
        else:
            return {
                "success": True,
                "table": table
                }

    def validate_status(self, status, new_status=False):
        """Validate a submission status.

        Arguments:
        status (dict): The status to validate.
        new_status (bool): Is this status a new status?

        Returns:
        dict:
            success: True if the status is valid, False if not.
            error: If the status is not valid, the reason why. Only present when success is False.
            details: Optional further details about an error.
        """
        # Validate against status schema
        try:
            jsonschema.validate(status, self.schema, resolver=self.resolver)
        except jsonschema.ValidationError as e:
            return {
                "success": False,
                "error": "Invalid status: {}".format(str(e).split("\n")[0]),
                "details": str(e)
            }

        code = status["code"]
        try:
            assert len(code) == len(self.STATUS_STEPS)
            if new_status:
                # Nothing started or finished
                assert code == "z" * len(code)
        except AssertionError:
            return {
                "success": False,
                "error": ("Invalid status code '{}' for {} status"
                          .format(code, "new" if new_status else "old"))
            }
        else:
            return {
                "success": True
            }

    def old_read_table(self, table_name, source_id):
        # Compatibility for legacy utils in this file
        tbl_res = self.old_get_dmo_table(table_name)
        if not tbl_res["success"]:
            return tbl_res
        table = tbl_res["table"]

        entry = table.get_item(Key={"source_id": source_id}, ConsistentRead=True).get(
            "Item")
        if not entry:
            return {
                "success": False,
                "error": "ID {} not found in {} database".format(source_id, table_name)
            }
        return {
            "success": True,
            "status": entry
        }

    def create_status(self, status):
        tbl_res = self.old_get_dmo_table("status")
        if not tbl_res["success"]:
            return tbl_res
        table = tbl_res["table"]

        # Add defaults
        status["messages"] = ["No message available"] * len(self.STATUS_STEPS)
        status["active"] = True
        status["cancelled"] = False
        status["pid"] = os.getpid()
        status["extensions"] = []
        status["hibernating"] = False
        status["code"] = "z" * len(self.STATUS_STEPS)
        status["updates"] = []

        status_valid = self.validate_status(status, new_status=True)
        if not status_valid["success"]:
            return status_valid

        # Check that status does not already exist
        if self.old_read_table("status", status["source_id"])["success"]:
            return {
                "success": False,
                "error": "ID {} already exists in status database".format(status["source_id"])
            }
        try:
            table.put_item(Item=status, ConditionExpression=Attr("source_id").not_exists())
        except Exception as e:
            return {
                "success": False,
                "error": repr(e)
            }
        else:
            logger.info("Status for {}: Created".format(status["source_id"]))
            return {
                "success": True,
                "status": status
            }
