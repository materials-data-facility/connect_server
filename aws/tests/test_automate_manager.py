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
            "API_CLIENT_SECRET": "hhhhs"
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
            "data_destinations": [
                "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/mdf_open/"
            ]
        })


    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items(self, _, secrets, organization):
        manager = AutomateManager(secrets)

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               submitting_user_id="12-33-55",
                                               test_submit=False)

        assert result['destination_endpoint_id'] == '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/MDF/mdf_connect/test_files/canonical_datasets/dft/'
        assert result['transfer_items'][0]['destination_path'] == '/mdf_open/'
        print(result)


    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items_from_origin(self, _, secrets, organization):
        manager = AutomateManager(secrets)

        data_sources = [
            "https://app.globus.org/file-manager?origin_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&origin_path=%2Fexalearn-design%2F"
        ]

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               submitting_user_id="12-33-55")

        assert result['destination_endpoint_id'] == '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/exalearn-design/'
        assert result['transfer_items'][0]['destination_path'] == '/mdf_open/'
        print(result)

    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items_test_submit(self, _, secrets, organization):
        manager = AutomateManager(secrets)

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               submitting_user_id="12-33-55",
                                               test_submit=True)

        assert result['destination_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/MDF/mdf_connect/test_files/canonical_datasets/dft/'
        assert result['transfer_items'][0]['destination_path'] == '/MDF/mdf_connect/test_files/deleteme_contents/'
        print(result)

    @mock.patch('automate_manager.GlobusAutomateFlow', autospec=True)
    def test_update_meta_only(self, mock_automate, secrets, organization, mocker):
        mock_flow = mocker.Mock()
        mock_automate.from_existing_flow = mocker.Mock(return_value=mock_flow)
        manager = AutomateManager(secrets)

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]
        user_token = {'access_token':'1234567890'}
        _ = manager.submit(mdf_rec=None, organization=organization,
               submitting_user_token=user_token, submitting_user_id = "12-33-55",
               data_sources = data_sources, do_curation=None, is_test=False, update_meta_only = True)

        mock_flow.run_flow.assert_called()
        print(mock_flow.run_flow.call_args[0])