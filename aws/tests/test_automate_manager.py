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

    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items(self, _, secrets):
        manager = AutomateManager(secrets)

        data_sources = [
            "https://app.globus.org/file-manager?destination_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&destination_path=%2FMDF%2Fmdf_connect%2Ftest_files%2Fcanonical_datasets%2Fdft%2F"
        ]

        organization = Organization.from_json_doc({
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
                "globus://e38ee745-6d04-11e5-ba46-22000b92c6ec/MDF/mdf_connect/test_files/deleteme_contents/"
            ]
        })

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               submitting_user_id="12-33-55")

        assert result['destination_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/MDF/mdf_connect/test_files/canonical_datasets/dft/'
        assert result['transfer_items'][0]['destination_path'] == '/MDF/mdf_connect/test_files/deleteme_contents/'
        print(result)


    @mock.patch('globus_automate_flow.GlobusAutomateFlow', autospec=True)
    def test_create_transfer_items_from_origin(self, _, secrets):
        manager = AutomateManager(secrets)

        data_sources = [
            "https://app.globus.org/file-manager?origin_id=e38ee745-6d04-11e5-ba46-22000b92c6ec&origin_path=%2Fexalearn-design%2F"
        ]

        organization = Organization.from_json_doc({
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
                "globus://e38ee745-6d04-11e5-ba46-22000b92c6ec/MDF/mdf_connect/test_files/deleteme_contents/"
            ]
        })

        result = manager.create_transfer_items(data_sources=data_sources,
                                               organization=organization,
                                               submitting_user_id="12-33-55")

        assert result['destination_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['source_endpoint_id'] == 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        assert result['submitting-user-id'] == '12-33-55'
        assert len(result['transfer_items']) == 1
        assert result['transfer_items'][0]['source_path'] == '/exalearn-design/'
        assert result['transfer_items'][0]['destination_path'] == '/MDF/mdf_connect/test_files/deleteme_contents/'
        print(result)
