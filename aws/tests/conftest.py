import json
from unittest import mock
from unittest.mock import patch

import pytest
from pytest_bdd import given, when, then

from mdf_connect_client import MDFConnectClient
from aws.submit_dataset import lambda_handler

fake_uuid = "abcdefgh-1234-4321-zyxw-hgfedcba"

@pytest.fixture
@mock.patch('mdf_connect_client.mdfcc.mdf_toolbox.login')
def mdf(_):
    mdf = MDFConnectClient()
    mdf.create_dc_block(title="How to make a dataset", authors=['Bob Dobolina'])
    mdf.add_data_source("https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F")
    return mdf

@given("I'm authenticated with MDF", target_fixture='mdf_environment')
def authenticated_with_globus(mocker):
    dynamo_manager = mocker.Mock()
    dynamo_manager.create_status = mocker.Mock(return_value={
        'success': True
    })

    automate_manager = mocker.Mock()
    automate_manager.submit = mocker.Mock(return_value='action-id-1')

    environment = {
        'dynamo_manager': dynamo_manager,
        'automate_manager': automate_manager,
        'authorizer': {
            'identities': "['me']",
            'user_id': 'my-id',
            'principalId': 'principal@foo.com',
            'name': 'Bob Dobolina',
            'globus_dependent_token': "{'70ab973f-da3f-49bb-9475-b1416aa588f8': '12sdfkj23-8j'}"
        }
    }
    return environment

@given('I have a new MDF dataset to submit', target_fixture='mdf_submission')
def mdf_datset(mdf, mdf_environment, mocker):
    mdf.update = False

    # No existing record
    mdf_environment['dynamo_manager'].get_current_version = mocker.Mock(return_value=None)

    # No provided source_id so use a uuid
    mdf_environment['source_id'] = fake_uuid

    return mdf.get_submission()

@when('I submit the dataset', target_fixture='submit_result')
def submit_dataset(mdf_environment, mdf_submission, mocker):
    dynamo_manager_class = mocker.Mock(return_value=mdf_environment['dynamo_manager'])
    if mdf_submission['update']:
        dynamo_manager_class.increment_record_version = mocker.Mock(return_value='1.1')
    else:
        dynamo_manager_class.increment_record_version = mocker.Mock(return_value='1.0')

    automate_manager_class = mocker.Mock(return_value=mdf_environment['automate_manager'])
    mocker.patch('aws.submit_dataset.get_secret')
    mock_uuid = mocker.patch('aws.submit_dataset.uuid.uuid4')
    mock_uuid.return_value = fake_uuid

    with patch('aws.submit_dataset.DynamoManager', new=dynamo_manager_class), \
            patch('aws.submit_dataset.AutomateManager', new=automate_manager_class):
        return lambda_handler({
            'requestContext': {'authorizer': mdf_environment['authorizer']},
            'headers': {'Authorization': 'Bearer 1209hkehjwerkhjre'},
            'body': json.dumps(mdf_submission)
        }, None)


@then('a dynamo record should be created with the provided source_id', target_fixture="dynamo_record")
@then('a dynamo record should be created with the original source_id', target_fixture="dynamo_record")
@then('a dynamo record should be created with the generated uuid', target_fixture="dynamo_record")
@then('a dynamo record should be created', target_fixture="dynamo_record")
def check_dynamo_record(mdf_environment):
    dynamo_manager = mdf_environment['dynamo_manager']
    dynamo_manager.create_status.assert_called()
    dynamo_record = dynamo_manager.create_status.call_args[0][0]
    assert dynamo_record['source_id'] == mdf_environment['source_id']
    assert dynamo_record['action_id'] == 'action-id-1'
    return dynamo_record


@then('an automate flow started', target_fixture="automate_record")
def check_automate_submission(mdf_environment):
    automate_manager = mdf_environment['automate_manager']
    automate_manager.submit.assert_called()
    automate_record = automate_manager.submit.call_args[1]
    assert automate_record['submitting_user_id'] == 'my-id'
    assert automate_record['submitting_user_token'] == '12sdfkj23-8j'
    assert not automate_record['update_meta_only']
    return automate_record


@then('I should receive a success result')
def no_error(submit_result):
    print("---------->", submit_result)
    assert submit_result['statusCode'] == 202
    body = json.loads(submit_result['body'])
    assert body['success']
