
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

    def get_dmo_table(self, table_name):
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

    def scan_table(self, table_name, fields=None, filters=None):
        """Scan the status or curation databases..

        Arguments:
        table_name (str): The Dynamo table to scan.
        fields (list of str): The fields from the results to return.
                              Default None, to return all fields.
        filters (list of tuples): The filters to apply. Format: (field, operator, value)
                                  For an entry to be returned, all filters must match.
                                  Default None, to return all entries.
                               field: The status field to filter on.
                               operator: The relation of field to value. Valid operators:
                                         ^: Begins with
                                         *: Contains
                                         ==: Equal to (or field does not exist, if value is None)
                                         !=: Not equal to (or field exists, if value is None)
                                         >: Greater than
                                         >=: Greater than or equal to
                                         <: Less than
                                         <=: Less than or equal to
                                         []: Between, inclusive (requires a list of two values)
                                         in: Is one of the values (requires a list of values)
                                             This operator effectively allows OR-ing '=='
                               value: The value of the field.

        Returns:
        dict: The results of the scan.
            success (bool): True on success, False otherwise.
            results (list of dict): The status entries returned.
            error (str): If success is False, the error that occurred.
        """
        # Get Dynamo status table
        tbl_res = self.get_dmo_table(table_name)
        print("Table", tbl_res)

        if not tbl_res["success"]:
            return tbl_res
        table = tbl_res["table"]

        # Translate fields
        if isinstance(fields, str) or fields is None:
            proj_exp = fields
        elif isinstance(fields, list):
            proj_exp = ",".join(fields)
        else:
            return {
                "success": False,
                "error": "Invalid fields type {}: '{}'".format(type(fields), fields)
            }

        # Translate filters
        # 0 = field
        # 1 = operator
        # 2 = value
        if isinstance(filters, tuple):
            filters = [filters]
        if filters is None or (isinstance(filters, list) and len(filters) == 0):
            filter_exps = None
        elif isinstance(filters, list):
            filter_exps = []
            for fil in filters:
                # Begins with
                if fil[1] == "^":
                    filter_exps.append(Attr(fil[0]).begins_with(fil[2]))
                # Contains
                elif fil[1] == "*":
                    filter_exps.append(Attr(fil[0]).contains(fil[2]))
                # Equal to (or field does not exist, if value is None)
                elif fil[1] == "==":
                    if fil[2] is None:
                        filter_exps.append(Attr(fil[0]).not_exists())
                    else:
                        filter_exps.append(Attr(fil[0]).eq(fil[2]))
                # Not equal to (or field exists, if value is None)
                elif fil[1] == "!=":
                    if fil[2] is None:
                        filter_exps.append(Attr(fil[0]).exists())
                    else:
                        filter_exps.append(Attr(fil[0]).ne(fil[2]))
                # Greater than
                elif fil[1] == ">":
                    filter_exps.append(Attr(fil[0]).gt(fil[2]))
                # Greater than or equal to
                elif fil[1] == ">=":
                    filter_exps.append(Attr(fil[0]).gte(fil[2]))
                # Less than
                elif fil[1] == "<":
                    filter_exps.append(Attr(fil[0]).lt(fil[2]))
                # Less than or equal to
                elif fil[1] == "<=":
                    filter_exps.append(Attr(fil[0]).lte(fil[2]))
                # Between, inclusive (requires a list of two values)
                elif fil[1] == "[]":
                    if not isinstance(fil[2], list) or len(fil[2]) != 2:
                        return {
                            "success": False,
                            "error": "Invalid between ('[]') operator values: '{}'".format(
                                fil[2])
                        }
                    filter_exps.append(Attr(fil[0]).between(fil[2][0], fil[2][1]))
                # Is one of the values (requires a list of values)
                elif fil[1] == "in":
                    if not isinstance(fil[2], list):
                        return {
                            "success": False,
                            "error": "Invalid 'in' operator values: '{}'".format(fil[2])
                        }
                    filter_exps.append(Attr(fil[0]).is_in(fil[2]))
                else:
                    return {
                        "success": False,
                        "error": "Invalid filter operator '{}'".format(fil[1])
                    }
        else:
            return {
                "success": False,
                "error": "Invalid filters type {}: '{}'".format(type(filters), filters)
            }

        # Make scan arguments
        scan_args = {
            "ConsistentRead": True
        }
        if proj_exp is not None:
            scan_args["ProjectionExpression"] = proj_exp
        if filter_exps is not None:
            # Create valid FilterExpression
            # Each Attr must be combined with &
            filter_expression = filter_exps[0]
            for i in range(1, len(filter_exps)):
                filter_expression = filter_expression & filter_exps[i]
            scan_args["FilterExpression"] = filter_expression

        # Make scan call, paging through if too many entries are scanned
        result_entries = []
        print("Scan ", scan_args)
        while True:
            scan_res = table.scan(**scan_args)
            # Check for success
            if scan_res["ResponseMetadata"]["HTTPStatusCode"] >= 300:
                return {
                    "success": False,
                    "error": ("HTTP code {} returned: {}"
                              .format(scan_res["ResponseMetadata"]["HTTPStatusCode"],
                                      scan_res["ResponseMetadata"]))
                }
            # Add results to list
            result_entries.extend(scan_res["Items"])
            # Check for completeness
            # If LastEvaluatedKey exists, need to page through more results
            if scan_res.get("LastEvaluatedKey", None) is not None:
                scan_args["ExclusiveStartKey"] = scan_res["LastEvaluatedKey"]
            # Otherwise, all results retrieved
            else:
                break

        return {
            "success": True,
            "results": result_entries
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
        tbl_res = self.get_dmo_table(table_name)
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
        tbl_res = self.get_dmo_table("status")
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
