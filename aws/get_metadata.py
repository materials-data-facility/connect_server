import json
import logging
import os

from dynamo_manager import DynamoManager

logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    """
    Handle GET /datasets/{source_id}/metadata requests.
    
    Path Parameters:
    - source_id: The dataset source ID
    
    Query Parameters:
    - version: Optional specific version (defaults to latest)
    
    Returns the complete metadata for a dataset with edit permissions.
    """
    print(json.dumps(event))
    
    # Extract user information from authorizer
    user_id = event['requestContext']['authorizer']['user_id']
    user_email = event['requestContext']['authorizer']['user_email']
    user_groups = eval(event['requestContext']['authorizer']['group_info'])
    identities = eval(event['requestContext']['authorizer']['identities'])
    
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
    
    # Parse query parameters
    query_params = event.get('queryStringParameters') or {}
    version = query_params.get('version')
    
    dynamo_manager = DynamoManager()
    
    try:
        # Get dataset metadata
        dataset = dynamo_manager.get_dataset_metadata(source_id, version)
        
        if not dataset:
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
        
        # Check if user has permission to view this dataset
        dataset_user_id = dataset.get('user_id')
        dataset_organization = dataset.get('organization')
        
        # Allow access if:
        # 1. User is the original submitter
        # 2. User is admin (has admin groups)
        # 3. Dataset is in user's organization (future enhancement)
        can_view = False
        can_edit = False
        
        if dataset_user_id and any(uid == dataset_user_id for uid in identities):
            can_view = True
            can_edit = True
        elif 'admin' in user_groups or 'mdf_admin' in user_groups:
            can_view = True
            can_edit = True
        # Add organization-based permissions here if needed
        
        if not can_view:
            return {
                'statusCode': 403,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Access denied'
                })
            }
        
        # Extract metadata and add permission information
        response_data = {
            'source_id': dataset['source_id'],
            'version': dataset['version'],
            'status': dataset.get('status', 'unknown'),
            'updated_at': dataset.get('updated_at'),
            'created_at': dataset.get('created_at'),
            'organization': dataset.get('organization'),
            'user_id': dataset.get('user_id'),
            'can_edit': can_edit,
            'metadata': dataset.get('dataset_mdata', {})
        }
        
        # Add version information
        if 'previous_versions' in dataset:
            response_data['previous_versions'] = dataset['previous_versions']
        
        # Add processing status information
        if 'code' in dataset:
            response_data['processing_status'] = dataset['code']
        if 'messages' in dataset:
            response_data['processing_messages'] = dataset['messages']
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'success': True,
                'dataset': response_data
            })
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