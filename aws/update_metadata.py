import json
import logging
import os
import uuid
from datetime import datetime
from copy import deepcopy

from dynamo_manager import DynamoManager
from metadata_validator import MetadataValidator
from source_id_manager import SourceIDManager
from automate_manager import AutomateManager
from utils import get_secret

logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    """
    Handle PATCH /datasets/{source_id}/metadata requests.
    
    Path Parameters:
    - source_id: The dataset source ID to update
    
    Request Body:
    JSON object with field updates, e.g.:
    {
        "dc.titles": [{"title": "New Title", "titleType": ""}],
        "dc.creators": [{"creatorName": "New Author"}],
        "dc.descriptions": [{"description": "New description", "descriptionType": "Abstract"}],
        "dc.subjects": ["tag1", "tag2", "tag3"]
    }
    
    Updates specified metadata fields and triggers reprocessing workflow.
    """
    print(json.dumps(event))
    
    # Extract user information from authorizer
    user_id = event['requestContext']['authorizer']['user_id']
    user_email = event['requestContext']['authorizer']['user_email']
    user_groups = eval(event['requestContext']['authorizer']['group_info'])
    identities = eval(event['requestContext']['authorizer']['identities'])
    name = event['requestContext']['authorizer']['name']
    
    depends = event['requestContext']['authorizer']['globus_dependent_token'].replace('null', 'None')
    globus_dependent_token = eval(depends)
    
    # Extract source_id from path parameters
    path_params = event.get('pathParameters') or {}
    source_id = path_params.get('source_id')
    
    if not source_id:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'success': False,
                'error': 'source_id path parameter is required'
            })
        }
    
    # Parse request body
    try:
        if not event.get('body'):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Request body is required'
                })
            }
        
        field_updates = json.loads(event['body'])
        
        if not isinstance(field_updates, dict) or not field_updates:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Request body must be a non-empty JSON object'
                })
            }
            
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'success': False,
                'error': 'Request body must be valid JSON'
            })
        }
    
    # Initialize managers
    dynamo_manager = DynamoManager()
    validator = MetadataValidator()
    sourceid_manager = SourceIDManager()
    automate_manager = AutomateManager()
    
    try:
        # Get current dataset
        current_dataset = dynamo_manager.get_dataset_metadata(source_id)
        
        if not current_dataset:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Dataset not found'
                })
            }
        
        # Check permissions - user must be original submitter or admin
        dataset_user_id = current_dataset.get('user_id')
        is_admin = 'admin' in user_groups or 'mdf_admin' in user_groups
        is_owner = dataset_user_id and any(uid == dataset_user_id for uid in identities)
        
        if not (is_owner or is_admin):
            return {
                'statusCode': 403,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Only the original submitter or administrators can update metadata'
                })
            }
        
        # Validate field permissions
        user_role = 'admin' if is_admin else 'user'
        permission_result = validator.validate_field_permissions(field_updates, user_role)
        
        if not permission_result['success']:
            return {
                'statusCode': 403,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Permission denied',
                    'details': permission_result['errors']
                })
            }
        
        # Validate field values
        validation_result = validator.validate_field_values(field_updates)
        
        if not validation_result['success']:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Invalid field values',
                    'details': validation_result['errors']
                })
            }
        
        # Apply metadata updates
        original_metadata = current_dataset.get('dataset_mdata', {})
        updated_metadata = validator.apply_metadata_updates(original_metadata, field_updates)
        
        # Validate complete updated metadata
        schema_validation = validator.validate_complete_metadata(updated_metadata)
        
        if not schema_validation['success']:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Updated metadata fails schema validation',
                    'details': schema_validation['errors']
                })
            }
        
        # Generate new version
        current_version = current_dataset['version']
        new_version = dynamo_manager.increment_record_version(current_version)
        
        if not new_version:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Failed to generate new version'
                })
            }
        
        # Create new dataset record with updated metadata
        new_dataset = deepcopy(current_dataset)
        new_dataset['version'] = new_version
        new_dataset['dataset_mdata'] = updated_metadata
        new_dataset['updated_at'] = datetime.utcnow().isoformat() + "Z"
        new_dataset['update_metadata_only'] = True
        
        # Update versioned source ID
        source_name = updated_metadata['mdf']['source_name']
        new_dataset['versioned_source_id'] = f"{source_name}_v{new_version}"
        updated_metadata['mdf']['versioned_source_id'] = new_dataset['versioned_source_id']
        updated_metadata['mdf']['version'] = new_version
        
        # Update previous versions list
        if 'previous_versions' in current_dataset:
            new_dataset['previous_versions'] = current_dataset['previous_versions'] + [f"{source_name}-{current_version}"]
        else:
            new_dataset['previous_versions'] = [f"{source_name}-{current_version}"]
        
        # Save new version to database
        create_result = dynamo_manager.create_status(new_dataset)
        
        if not create_result['success']:
            logger.error(f"Failed to create new dataset version: {create_result['error']}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Failed to save updated dataset'
                })
            }
        
        # Trigger metadata-only reprocessing workflow
        try:
            # Prepare flow input
            flow_input = {
                'source_id': source_id,
                'version': new_version,
                'versioned_source_id': new_dataset['versioned_source_id'],
                'dataset_mdata': updated_metadata,
                'update_metadata_only': True,
                'submitting_user_email': user_email,
                'submitting_user_name': name,
                'user_id': user_id,
                'organization': updated_metadata.get('mdf', {}).get('organization', ''),
                'api_triggered': True
            }
            
            # Start the flow
            run_as_scope = os.environ.get("RUN_AS_SCOPE")
            flow_result = automate_manager.start_flow(
                flow_input=flow_input,
                flow_scope=run_as_scope,
                run_as_token=globus_dependent_token
            )
            
            if not flow_result.get('success', False):
                logger.warning(f"Failed to start metadata update flow: {flow_result}")
                # Don't fail the request - the metadata is saved, just workflow didn't start
                
        except Exception as e:
            logger.warning(f"Failed to trigger metadata update workflow: {e}")
            # Continue - the metadata update was successful even if workflow failed
        
        # Generate diff for response
        metadata_diff = validator.generate_metadata_diff(original_metadata, updated_metadata)
        
        response_data = {
            'success': True,
            'source_id': source_id,
            'old_version': current_version,
            'new_version': new_version,
            'versioned_source_id': new_dataset['versioned_source_id'],
            'updated_at': new_dataset['updated_at'],
            'changes': metadata_diff,
            'metadata': updated_metadata
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'success': False,
                'error': 'Internal server error'
            })
        }