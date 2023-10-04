import os
from boto3.dynamodb.conditions import Key
from dynamo_manager import DynamoManager


class TestDynamoManager:
    def test_get_current_version(self, mocker):
        mock_dynamo = mocker.Mock()
        mock_table = mocker.Mock()
        mock_dynamo.Table = mocker.Mock(return_value=mock_table)

        batch1 = {
            "Items": [
                {"version": '1.1'},
                {"version": '1.12'},
                {"version": '1.2'},
            ],
            'LastEvaluatedKey': '3'
        }
        batch2 = {
            "Items": [
                {"version": '1.6'},
                {"version": '1.13'},
                {"version": '1.7'},
            ],
            'LastEvaluatedKey': None
        }

        mock_table.query = mocker.Mock(side_effect=[batch1, batch2])
        mock_boto = mocker.patch('dynamo_manager.boto3')
        mock_boto.client = mocker.Mock(return_value=mock_dynamo)

        os.environ["DYNAMO_STATUS_TABLE"] = 'test_table'
        os.environ["DYNAMO_CURATION_TABLE"] = 'test_curation_table'
        dynamo_manager = DynamoManager()
        record = dynamo_manager.get_current_version("test_submission")
        assert record['version'] == '1.13'

        query_calls = mock_table.query.call_args_list
        assert len(query_calls) == 2
        assert 'ExclusiveStartKey' not in query_calls[0][1]
        assert 'ExclusiveStartKey' in query_calls[1][1]
        assert query_calls[1][1]['ExclusiveStartKey'] == '3'
        assert query_calls[0][1]['KeyConditionExpression'] == Key('source_id').eq(
            'test_submission')

    def test_get_current_version_not_exist(self, mocker):
        mock_dynamo = mocker.Mock()
        mock_table = mocker.Mock()
        mock_dynamo.Table = mocker.Mock(return_value=mock_table)

        batch1 = {
            "Items": [
            ],
            'LastEvaluatedKey': None
        }

        mock_table.query = mocker.Mock(side_effect=[batch1])
        mock_boto = mocker.patch('dynamo_manager.boto3')
        mock_boto.client = mocker.Mock(return_value=mock_dynamo)

        os.environ["DYNAMO_STATUS_TABLE"] = 'test_table'
        os.environ["DYNAMO_CURATION_TABLE"] = 'test_curation_table'
        dynamo_manager = DynamoManager()
        record = dynamo_manager.get_current_version("test_submission")
        assert not record

    def test_increment_record_version(self):
        assert DynamoManager.increment_record_version("1.1") == "1.2"
        assert DynamoManager.increment_record_version("1.12") == "1.13"
        assert not DynamoManager.increment_record_version("1")
        assert DynamoManager.increment_record_version(None) == '1.0'
