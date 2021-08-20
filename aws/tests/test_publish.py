from pytest_bdd import scenario, given, then, parsers


@scenario('publish.feature', 'Submit Test Dataset')
def test_submit_test_dataset():
    pass


@given('I mark the dataset as test', target_fixture='mdf_submission')
def mdf_other_user_datset(mdf, mdf_submission):
    mdf.set_test(True)
    return mdf.get_submission()

@then(parsers.parse("the only data destinations should be {destination}"))
def dyanmo_record_version(destination, automate_record):
    print("Dest--<",destination)
    assert len(automate_record['data_sources']) == 1
