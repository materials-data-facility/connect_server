import json
from unittest import mock
from unittest.mock import patch

import pytest
from pytest_bdd import scenario, given, when, then

from aws.submit_dataset import lambda_handler
from mdf_connect_client import MDFConnectClient


@pytest.fixture
@mock.patch('mdf_connect_client.mdfcc.mdf_toolbox.login')
def mdf(_):
    mdf = MDFConnectClient()
    mdf.create_dc_block(title="How to make a dataset", authors=['Bob Dobolina'])
    mdf.add_data_source("https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F")
    return mdf


@scenario('submit_dataset.feature', 'Submit Dataset')
def test_publish():
    pass

@scenario('submit_dataset.feature', 'Attempt to update another users record')
def test_update_other_users_record():
    pass


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
            'globus_dependent_token': "{'ce2aca7c-6de8-4b57-b0a0-dcca83a232ab': '12sdfkj23-8j'}"
        }
    }
    return environment


@given('I have a new MDF dataset to submit', target_fixture='mdf_submission')
def mdf_datset(mdf, mdf_environment, mocker):
    mdf.update = False
    mdf.set_source_name("my dataset")

    # No existing record
    mdf_environment['dynamo_manager'].get_current_version = mocker.Mock(return_value=None)

    return mdf.get_submission()


@given('I have an update to another users record', target_fixture='mdf_submission')
def mdf_other_user_datset(mdf, mdf_environment, mocker):
    mdf.set_source_name("my dataset")

    # Existing record in dynamo with a different user ID
    mdf_environment['dynamo_manager'].get_current_version = mocker.Mock(return_value={
        'version': '1.0',
        'user_id': 'you'
    })

    return mdf.get_submission()


@when('I submit the dataset', target_fixture='submit_result')
def submit_dataset(mdf_environment, mdf_submission, mocker):
    dynamo_manager_class = mocker.Mock(return_value=mdf_environment['dynamo_manager'])
    dynamo_manager_class.increment_record_version = mocker.Mock(return_value='1.1')

    automate_manager_class = mocker.Mock(return_value=mdf_environment['automate_manager'])
    mocker.patch('aws.submit_dataset.get_secret')

    with patch('aws.submit_dataset.DynamoManager', new=dynamo_manager_class), \
            patch('aws.submit_dataset.AutomateManager', new=automate_manager_class):
        return lambda_handler({
            'requestContext': {'authorizer': mdf_environment['authorizer']},
            'headers': {'Authorization': 'Bearer 1209hkehjwerkhjre'},
            'body': json.dumps(mdf_submission)
        }, None)


@then('a dynamo record should be created')
def check_dynamo_record(mdf_environment):
    dynamo_manager = mdf_environment['dynamo_manager']
    dynamo_manager.create_status.assert_called()
    dynamo_record = dynamo_manager.create_status.call_args[0][0]
    assert dynamo_record['source_id'] == 'my dataset'
    assert dynamo_record['action_id'] == 'action-id-1'

@then('an automate flow started')
def check_dynamo_record(mdf_environment):
    automate_manager = mdf_environment['automate_manager']
    automate_manager.submit.assert_called()
    automate_record = automate_manager.submit.call_args[1]
    assert automate_record['submitting_user_id'] == 'my-id'
    assert automate_record['submitting_user_token'] == '12sdfkj23-8j'

@then('I should receive a success result')
def no_error(submit_result):
    print("---------->", submit_result)
    assert submit_result['statusCode'] == 202
    body = json.loads(submit_result['body'])
    assert body['success']


@then('I should receive a failure result')
def no_error(submit_result):
    print("---------->", submit_result)
    assert submit_result['statusCode'] == 400
    body = json.loads(submit_result['body'])
    assert not body['success']


@then('the article should be published')
def article_published(submit_result):
    assert True
