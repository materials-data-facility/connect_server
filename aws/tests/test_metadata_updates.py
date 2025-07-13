import json
import pytest
from unittest.mock import Mock, patch, MagicMock

# Import our modules
import metadata_validator
import list_datasets
import get_metadata
import update_metadata
import get_versions


class TestMetadataValidator:
    """Test the MetadataValidator class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        with patch('metadata_validator.os.environ.get') as mock_env:
            mock_env.return_value = './schemas/schemas'
            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = '{"type": "object"}'
                self.validator = metadata_validator.MetadataValidator()
    
    def test_validate_field_permissions_user_allowed(self):
        """Test that users can update allowed fields."""
        field_updates = {
            'dc.titles': [{'title': 'New Title'}],
            'dc.creators': [{'creatorName': 'New Author'}]
        }
        
        result = self.validator.validate_field_permissions(field_updates, 'user')
        assert result['success'] is True
        assert len(result['errors']) == 0
    
    def test_validate_field_permissions_user_restricted(self):
        """Test that users cannot update restricted fields."""
        field_updates = {
            'datacite.doi': '10.1234/test',
            'mdf.source_id': 'test_id'
        }
        
        result = self.validator.validate_field_permissions(field_updates, 'user')
        assert result['success'] is False
        assert len(result['errors']) == 2
        assert 'datacite.doi' in result['errors'][0]
        assert 'mdf.source_id' in result['errors'][1]
    
    def test_validate_field_permissions_admin_allowed(self):
        """Test that admins can update restricted fields."""
        field_updates = {
            'datacite.doi': '10.1234/test',
            'dc.titles': [{'title': 'New Title'}]
        }
        
        result = self.validator.validate_field_permissions(field_updates, 'admin')
        assert result['success'] is True
        assert len(result['errors']) == 0
    
    def test_validate_titles_valid(self):
        """Test validation of valid titles."""
        titles = [{'title': 'Valid Title', 'titleType': ''}]
        errors = self.validator._validate_titles(titles)
        assert len(errors) == 0
    
    def test_validate_titles_invalid(self):
        """Test validation of invalid titles."""
        # Empty titles list
        errors = self.validator._validate_titles([])
        assert 'At least one title is required' in errors[0]
        
        # Missing title field
        errors = self.validator._validate_titles([{'titleType': 'Main'}])
        assert 'must have a \'title\' field' in errors[0]
        
        # Empty title
        errors = self.validator._validate_titles([{'title': '   '}])
        assert 'must be a non-empty string' in errors[0]
    
    def test_apply_metadata_updates(self):
        """Test applying field updates to metadata."""
        original_metadata = {
            'dc': {
                'titles': [{'title': 'Old Title'}],
                'creators': [{'creatorName': 'Old Author'}]
            },
            'mdf': {
                'ingest_date': '2023-01-01T00:00:00Z'
            }
        }
        
        field_updates = {
            'dc.titles': [{'title': 'New Title'}],
            'dc.descriptions': [{'description': 'New description', 'descriptionType': 'Abstract'}]
        }
        
        updated_metadata = self.validator.apply_metadata_updates(original_metadata, field_updates)
        
        assert updated_metadata['dc']['titles'][0]['title'] == 'New Title'
        assert updated_metadata['dc']['descriptions'][0]['description'] == 'New description'
        assert updated_metadata['dc']['creators'][0]['creatorName'] == 'Old Author'  # Unchanged
        assert updated_metadata['mdf']['ingest_date'] != '2023-01-01T00:00:00Z'  # Updated timestamp
    
    def test_generate_metadata_diff(self):
        """Test generating metadata diff between versions."""
        original_metadata = {
            'dc': {
                'titles': [{'title': 'Old Title'}],
                'creators': [{'creatorName': 'Same Author'}]
            }
        }
        
        updated_metadata = {
            'dc': {
                'titles': [{'title': 'New Title'}],
                'creators': [{'creatorName': 'Same Author'}]
            }
        }
        
        diff = self.validator.generate_metadata_diff(original_metadata, updated_metadata)
        
        assert 'dc.titles' in diff
        assert diff['dc.titles']['old'][0]['title'] == 'Old Title'
        assert diff['dc.titles']['new'][0]['title'] == 'New Title'
        assert 'dc.creators' not in diff  # No change


class TestListDatasets:
    """Test the list_datasets lambda function."""
    
    @patch('list_datasets.DynamoManager')
    def test_list_datasets_success(self, mock_dynamo_class):
        """Test successful dataset listing."""
        # Mock DynamoDB response
        mock_dynamo = Mock()
        mock_dynamo_class.return_value = mock_dynamo
        mock_dynamo.query_user_datasets.return_value = {
            'success': True,
            'results': [
                {
                    'source_id': 'test_dataset_1',
                    'version': '1.0',
                    'status': 'active',
                    'updated_at': '2023-01-01T00:00:00Z',
                    'dataset_mdata': {
                        'dc': {
                            'titles': [{'title': 'Test Dataset 1'}],
                            'creators': [{'creatorName': 'Test Author'}],
                            'descriptions': [{'description': 'Test description'}],
                            'subjects': ['test', 'dataset']
                        }
                    }
                }
            ],
            'last_key': None,
            'count': 1
        }
        
        # Mock event
        event = {
            'requestContext': {
                'authorizer': {
                    'user_id': 'test_user',
                    'user_email': 'test@example.com',
                    'group_info': "[]",
                    'identities': "['test_user']"
                }
            },
            'queryStringParameters': {'limit': '10'}
        }
        
        result = list_datasets.lambda_handler(event, {})
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['success'] is True
        assert len(body['datasets']) == 1
        assert body['datasets'][0]['title'] == 'Test Dataset 1'
        assert body['datasets'][0]['first_author'] == 'Test Author'
    
    @patch('list_datasets.DynamoManager')
    def test_list_datasets_database_error(self, mock_dynamo_class):
        """Test database error handling."""
        mock_dynamo = Mock()
        mock_dynamo_class.return_value = mock_dynamo
        mock_dynamo.query_user_datasets.return_value = {
            'success': False,
            'error': 'Database connection failed'
        }
        
        event = {
            'requestContext': {
                'authorizer': {
                    'user_id': 'test_user',
                    'user_email': 'test@example.com',
                    'group_info': "[]",
                    'identities': "['test_user']"
                }
            },
            'queryStringParameters': {}
        }
        
        result = list_datasets.lambda_handler(event, {})
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['success'] is False
        assert 'Database query failed' in body['error']


class TestUpdateMetadata:
    """Test the update_metadata lambda function."""
    
    @patch('update_metadata.AutomateManager')
    @patch('update_metadata.SourceIDManager')
    @patch('update_metadata.MetadataValidator')
    @patch('update_metadata.DynamoManager')
    def test_update_metadata_success(self, mock_dynamo_class, mock_validator_class, 
                                   mock_sourceid_class, mock_automate_class):
        """Test successful metadata update."""
        # Mock dependencies
        mock_dynamo = Mock()
        mock_dynamo_class.return_value = mock_dynamo
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        mock_sourceid = Mock()
        mock_sourceid_class.return_value = mock_sourceid
        mock_automate = Mock()
        mock_automate_class.return_value = mock_automate
        
        # Mock current dataset
        current_dataset = {
            'source_id': 'test_dataset',
            'version': '1.0',
            'user_id': 'test_user',
            'dataset_mdata': {
                'dc': {'titles': [{'title': 'Old Title'}]},
                'mdf': {'source_name': 'test_dataset'}
            }
        }
        mock_dynamo.get_dataset_metadata.return_value = current_dataset
        mock_dynamo.increment_record_version.return_value = '1.1'
        mock_dynamo.create_status.return_value = {'success': True}
        
        # Mock validator responses
        mock_validator.validate_field_permissions.return_value = {'success': True, 'errors': []}
        mock_validator.validate_field_values.return_value = {'success': True, 'errors': []}
        mock_validator.validate_complete_metadata.return_value = {'success': True, 'errors': []}
        mock_validator.apply_metadata_updates.return_value = {
            'dc': {'titles': [{'title': 'New Title'}]},
            'mdf': {'source_name': 'test_dataset', 'ingest_date': '2023-01-01T00:00:00Z'}
        }
        mock_validator.generate_metadata_diff.return_value = {
            'dc.titles': {
                'old': [{'title': 'Old Title'}],
                'new': [{'title': 'New Title'}]
            }
        }
        
        # Mock automate manager
        mock_automate.start_flow.return_value = {'success': True}
        
        # Mock event
        event = {
            'requestContext': {
                'authorizer': {
                    'user_id': 'test_user',
                    'user_email': 'test@example.com',
                    'group_info': "[]",
                    'identities': "['test_user']",
                    'name': 'Test User',
                    'globus_dependent_token': 'None'
                }
            },
            'pathParameters': {'source_id': 'test_dataset'},
            'body': json.dumps({
                'dc.titles': [{'title': 'New Title'}]
            })
        }
        
        with patch('update_metadata.os.environ.get') as mock_env:
            mock_env.return_value = 'test_scope'
            result = update_metadata.lambda_handler(event, {})
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['success'] is True
        assert body['old_version'] == '1.0'
        assert body['new_version'] == '1.1'
        assert 'dc.titles' in body['changes']
    
    @patch('update_metadata.DynamoManager')
    def test_update_metadata_permission_denied(self, mock_dynamo_class):
        """Test permission denied for non-owner."""
        mock_dynamo = Mock()
        mock_dynamo_class.return_value = mock_dynamo
        
        # Mock dataset owned by different user
        current_dataset = {
            'source_id': 'test_dataset',
            'version': '1.0',
            'user_id': 'other_user',
            'dataset_mdata': {}
        }
        mock_dynamo.get_dataset_metadata.return_value = current_dataset
        
        event = {
            'requestContext': {
                'authorizer': {
                    'user_id': 'test_user',
                    'user_email': 'test@example.com',
                    'group_info': "[]",
                    'identities': "['test_user']"
                }
            },
            'pathParameters': {'source_id': 'test_dataset'},
            'body': json.dumps({'dc.titles': [{'title': 'New Title'}]})
        }
        
        result = update_metadata.lambda_handler(event, {})
        
        assert result['statusCode'] == 403
        body = json.loads(result['body'])
        assert body['success'] is False
        assert 'original submitter' in body['error']


if __name__ == '__main__':
    pytest.main([__file__])