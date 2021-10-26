import json

from pytest_bdd import scenario, given, then, parsers


@scenario('submit_dataset.feature', 'Submit Dataset With Provided source_id')
def test_publish_provided_source_id():
    pass

@scenario('submit_dataset.feature', 'Submit Dataset')
def test_publish():
    pass

@scenario('submit_dataset.feature', 'Attempt to update another users record')
def test_update_other_users_record():
    pass


@scenario('submit_dataset.feature', 'Attempt to add a record with an existing source_id')
def test_add_record_with_existing_source_id():
    pass


@scenario('submit_dataset.feature', 'Update a submitted dataset')
def test_update_existing_record():
    pass


@scenario('submit_dataset.feature', 'Update metadata only for a submitted dataset')
def test_update_metadata_only():
    pass


@given('I have an update to another users record', target_fixture='mdf_submission')
def mdf_other_user_datset(mdf, mdf_environment, mocker):
    mdf.set_source_name("my dataset")
    mdf_environment['source_id'] = 'my dataset'

    # Existing record in dynamo with a different user ID
    mdf_environment['dynamo_manager'].get_current_version = mocker.Mock(return_value={
        'version': '1.0',
        'user_id': 'you'
    })

    return mdf.get_submission()


@given('I have an update for an existing dataset', target_fixture='mdf_submission')
def mdf_other_user_datset(mdf, mdf_environment, mocker):
    mdf.set_source_name("my dataset")
    mdf_environment['source_id'] = 'my dataset'
    mdf.update = True

    # Existing record in dynamo with a same user ID
    mdf_environment['dynamo_manager'].get_current_version = mocker.Mock(return_value={
        'version': '1.0',
        'user_id': 'me'
    })

    return mdf.get_submission()


@given("I provide the source_id", target_fixture='mdf_submission')
def provided_source_id(mdf, mdf_environment):
    mdf.set_source_name("my dataset")
    mdf_environment['source_id'] = 'my dataset'
    return mdf.get_submission()


@given('I have a new MDF dataset to submit with a source_id that already exists',
       target_fixture='mdf_submission')
def mdf_other_user_datset(mdf, mdf_environment, mocker):
    mdf.set_source_name("my dataset")
    mdf_environment['source_id'] = 'my dataset'

    # Existing record in dynamo with a different user ID
    mdf_environment['dynamo_manager'].get_current_version = mocker.Mock(return_value={
        'version': '1.0',
        'user_id': 'me'
    })

    return mdf.get_submission()


@given('I have a metadata only update for an existing dataset', target_fixture='mdf_submission')
def mdf_other_user_datset(mdf, mdf_environment, mocker):
    mdf.set_source_name("my dataset")
    mdf_environment['source_id'] = 'my dataset'
    mdf.update = True
    mdf_environment['update_meta_only'] = True
    mdf.set_update_metadata_only(True)


    # Existing record in dynamo with a same user ID
    mdf_environment['dynamo_manager'].get_current_version = mocker.Mock(return_value={
        'version': '1.0',
        'user_id': 'me'
    })

    return mdf.get_submission()







@then('I should receive a success result with the provided source_id')
@then('I should receive a success result with the generated uuid')
def no_error(submit_result, mdf_environment):
    print("---------->", submit_result)
    assert submit_result['statusCode'] == 202
    body = json.loads(submit_result['body'])
    assert body['success']
    assert body['source_id'] == mdf_environment['source_id']


@then('I should receive a failure result')
def no_error(submit_result):
    print("---------->", submit_result)
    assert submit_result['statusCode'] == 400
    body = json.loads(submit_result['body'])
    assert not body['success']


@then('the article should be published')
def article_published(submit_result):
    assert True


@then(parsers.parse("the dynamo record should be version {version_num}"))
def dyanmo_record_version(version_num, dynamo_record):
    assert dynamo_record['version'] == version_num


@then("the data destination should be the Petrel MDF directory")
def check_data_dest(automate_record):
    print("Autaomte", automate_record)

@then('an automate flow started that skips the file transfer', target_fixture="automate_record")
def check_skip_file_transfer(mdf_environment):
    automate_manager = mdf_environment['automate_manager']
    automate_manager.submit.assert_called()
    automate_record = automate_manager.submit.call_args[1]
    print("automate_record:")
    print(automate_record)
    print("atuomate_manager")
    print(automate_manager)
    print("mdf_env")
    print(mdf_environment)
    assert automate_record['submitting_user_id'] == 'my-id'
    assert automate_record['submitting_user_token'] == '12sdfkj23-8j'
    assert automate_record['update_meta_only']
    return automate_record