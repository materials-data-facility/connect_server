from pytest_bdd import scenario, given, then, parsers


@scenario('publish.feature', 'Submit Test Dataset')
def test_submit_test_dataset():
    pass

@scenario('publish.feature', 'Submit Dataset for Organization')
def test_submit_dataset_for_org():
    pass


@given('I mark the dataset as test', target_fixture='mdf_submission')
def mdf_other_user_datset(mdf):
    mdf.set_test(True)
    return mdf.get_submission()


@then(parsers.parse("the automate flow will send the files to mdf_connect/test_files"))
def dyanmo_record_version(automate_record):
    assert automate_record['is_test']


@then(parsers.parse("the only data destinations should be {destination}"))
def dyanmo_record_version(destination, automate_record):
    dest_org = automate_record['organization']
    print("dest org", dest_org)
    assert len(dest_org.data_destinations) == 1


@then(parsers.parse("the dataset's domain should be '{domain}'"))
def check_dataset_domain(domain, automate_record):
    assert automate_record['mdf_rec']['mdf']['domains'][0] == domain


@given("I set the organization to VERDE", target_fixture='mdf_submission')
def set_org_verde(mdf):
    mdf.set_organization("VERDE")
    return mdf.get_submission()

