import os
from unittest import mock

import pytest

from automate_manager import AutomateManager
from organization import Organization


class TestAutomateManager:
    @pytest.fixture
    def secrets(self):
        return {
            "SES_ACCESS_KEY": "123-55",
            "SES_SECRET": "shhh",
            "API_CLIENT_ID": "55-321",
            "API_CLIENT_SECRET": "hhhhs",
            "DATACITE_USERNAME_PROD": "datacite_prod_usrname_1234",
            "DATACITE_PASSWORD_PROD": "datacite_prod_passwrd_1234",
            "DATACITE_PREFIX_PROD": "10.12345",
            "DATACITE_USERNAME_TEST": "datacite_test_usrname_1234",
            "DATACITE_PASSWORD_TEST": "datacite_test_passwrd_1234",
            "DATACITE_PREFIX_TEST": "10.12347"
        }
    

    @pytest.fixture
    def organization(self):
        return Organization.from_json_doc({
            "canonical_name": "MDF Open",
            "aliases": [
                "Open"
            ],
            "description": "A template for open and published data.",
            "permission_groups": [
                "cc192dca-3751-11e8-90c1-0a7c735d220a"
            ],
            "acl": [
                "public"
            ],
            "curation": True,
            "mint_doi": False,
            "data_destinations": [
                "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/mdf_open/"
            ]
        })

    @pytest.fixture
    def organization_mint_doi(self):
        return Organization.from_json_doc({
            "canonical_name": "MDF Open",
            "aliases": [
                "Open"
            ],
            "description": "A template for open and published data that mints dois.",
            "permission_groups": [
                "cc192dca-3751-11e8-90c1-0a7c735d220a"
            ],
            "acl": [
                "public"
            ],
            "curation": True,
            "mint_doi": True,
            "data_destinations": [
                "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/mdf_open/"
            ]
        })


    @pytest.fixture
    def set_environ(self):
        os.environ['MANAGE_FLOWS_SCOPE'] = 'https://manage.flows.scope.123'
        os.environ['TEST_DATA_DESTINATION'] = "globus://test_data"
        os.environ['FLOW_ID'] = 'flow-id-1'
        os.environ['FLOW_SCOPE'] = 'flow-scope-1'

    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items(self, _, secrets, organization, set_environ):
        os.environ['PORTAL_URL'] = "https://acdc.alcf.anl.gov/mdf/detail/"
        manager = AutomateManager(secrets, is_test=False)

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               submitting_user_id="12-33-55",
                                               source_id="myTestDataset",
                                               version="1.0.1",
                                               test_submit=False)

        assert result['destination_endpoint_id'] == '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/MDF/mdf_connect/test_files/canonical_datasets/dft/'
        assert result['transfer_items'][0]['destination_path'] == '/mdf_open/myTestDataset/1.0.1/'
        assert result['transfer_items'][0]['dataset_path'] == '/mdf_open/myTestDataset/'
        print(result)


    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items_from_origin(self, _, secrets, organization):
        os.environ['PORTAL_URL'] = "https://acdc.alcf.anl.gov/mdf/detail/"
        manager = AutomateManager(secrets, is_test=False)

        data_sources = [
            "https://app.globus.org/file-manager?origin_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&origin_path=%2Fexalearn-design%2F"
        ]

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               source_id="myTestDataset",
                                               version="1.0.1",
                                               submitting_user_id="12-33-55")

        assert result['destination_endpoint_id'] == '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/exalearn-design/'
        assert result['transfer_items'][0]['destination_path'] == '/mdf_open/myTestDataset/1.0.1/'
        print(result)

    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items_from_google_drive(self, _, secrets, organization):
        os.environ['PORTAL_URL'] = "https://acdc.alcf.anl.gov/mdf/detail/"
        os.environ['GDRIVE_EP'] = "f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb"
        os.environ['GDRIVE_ROOT'] = "/Shared With Me"
        manager = AutomateManager(secrets, is_test=False)

        data_sources = [
            "google:///mdf/my_dataset"
        ]

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               source_id="myTestDataset",
                                               version="1.0.1",
                                               submitting_user_id="12-33-55")

        assert result['destination_endpoint_id'] == '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec'
        assert result['source_endpoint_id'] == 'f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/Shared With Me/mdf/my_dataset'
        assert result['transfer_items'][0]['destination_path'] == '/mdf_open/myTestDataset/1.0.1/'
        print(result)

    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items_test_submit(self, _, secrets, organization, set_environ):
        os.environ['PORTAL_URL'] = "https://acdc.alcf.anl.gov/mdf/detail/"
        manager = AutomateManager(secrets, is_test=True)

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               submitting_user_id="12-33-55",
                                               source_id="myTestDataset",
                                               version="1.0.1",
                                               test_submit=True)

        assert result['destination_endpoint_id'] == 'test_data'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/MDF/mdf_connect/test_files/canonical_datasets/dft/'
        assert result['transfer_items'][0]['destination_path'] == 'myTestDataset/1.0.1/'
        print(result)

    @mock.patch('automate_manager.GlobusAutomateFlow', autospec=True)
    def test_update_metadata_only(self, mock_automate, secrets, organization, mocker, mdf_rec):
        mock_flow = mocker.Mock()
        mock_automate.from_existing_flow = mocker.Mock(return_value=mock_flow)
        os.environ['PORTAL_URL'] = "https://acdc.alcf.anl.gov/mdf/detail/"
        manager = AutomateManager(secrets, is_test=False)

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]
        user_token = {'access_token': '1234567890'}
        _ = manager.submit(mdf_rec=mdf_rec, organization=organization,
                           submitting_user_token=user_token,
                           submitting_user_id="12-33-55", monitor_by_id=["12-33-55",
                                                                         "5fc63928-3752-11e8-9c6f-0e00fd09bf20"],
                           submitting_user_email="foo@bar.com",
                           search_index_uuid="098-765-4321",
                           data_sources=data_sources, is_test=False,
                           update_metadata_only=True)

        mock_flow.run_flow.assert_called()
        assert(mock_flow.run_flow.call_args[0][0]['update_metadata_only'])
    

    @mock.patch('automate_manager.GlobusAutomateFlow', autospec=True)
    def test_mint_doi(self, mock_automate, secrets, organization_mint_doi, mocker, mdf_rec):
        mock_flow = mocker.Mock()
        mock_automate.from_existing_flow = mocker.Mock(return_value=mock_flow)
        os.environ['PORTAL_URL'] = "https://acdc.alcf.anl.gov/mdf/detail/"
        manager = AutomateManager(secrets, is_test=False)
        assert manager.datacite_username == "datacite_prod_usrname_1234"
        assert manager.datacite_password == "datacite_prod_passwrd_1234"
        assert manager.datacite_prefix == "10.12345"

        manager = AutomateManager(secrets, is_test=True)
        assert manager.datacite_username == "datacite_test_usrname_1234"
        assert manager.datacite_password == "datacite_test_passwrd_1234"
        assert manager.datacite_prefix == "10.12347"

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]
        user_token = {'access_token': '1234567890'}
        _ = manager.submit(mdf_rec=mdf_rec, organization=organization_mint_doi,
                           submitting_user_token=user_token,
                           submitting_user_id="12-33-55", monitor_by_id=["12-33-55",
                                                                         "5fc63928-3752-11e8-9c6f-0e00fd09bf20"],
                           submitting_user_email="foo@bar.com",
                           search_index_uuid='098-765-4321',
                           data_sources=data_sources, is_test=False,
                           update_metadata_only=False)

        mock_flow.run_flow.assert_called()
        assert(mock_flow.run_flow.call_args[0][0]['mint_doi'])

