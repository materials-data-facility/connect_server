import json
import logging
import os

from dynamo_manager import DynamoManager
from metadata_validator import MetadataValidator

logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    """
    Handle GET /datasets/{source_id}/versions requests.
    
    Path Parameters:
    - source_id: The dataset source ID
    
    Query Parameters:
    - limit: Maximum number of versions to return (default 50, max 100)
    - include_diff: Include metadata diff between versions (default false)
    
    Returns version history for a dataset with optional metadata diffs.
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
    limit = min(int(query_params.get('limit', 50)), 100)  # Cap at 100
    include_diff = query_params.get('include_diff', '').lower() == 'true'
    
    dynamo_manager = DynamoManager()
    
    try:
        # Get all versions of the dataset
        result = dynamo_manager.get_dataset_versions(source_id, limit)
        
        if not result['success']:
            logger.error(f"Database query failed: {result['error']}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Database query failed'
                })
            }
        
        versions = result['results']
        
        if not versions:
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
        dataset_user_id = versions[0].get('user_id')  # All versions have same user_id
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
                    'error': 'Access denied'
                })
            }
        
        # Process versions for response
        version_list = []
        validator = MetadataValidator() if include_diff else None
        
        for i, version_data in enumerate(versions):
            version_info = {
                'version': version_data['version'],
                'versioned_source_id': version_data.get('versioned_source_id', ''),
                'status': version_data.get('status', 'unknown'),
                'updated_at': version_data.get('updated_at'),
                'created_at': version_data.get('created_at'),
                'is_latest': i == 0,  # First item is latest (sorted descending)
                'processing_status': version_data.get('code', ''),
                'active': version_data.get('active', False)
            }
            
            # Extract key metadata changes for summary
            if 'dataset_mdata' in version_data:
                mdata = version_data['dataset_mdata']
                
                # Get title
                if 'dc' in mdata and 'titles' in mdata['dc'] and mdata['dc']['titles']:
                    version_info['title'] = mdata['dc']['titles'][0].get('title', 'Untitled')
                
                # Get first author
                if 'dc' in mdata and 'creators' in mdata['dc'] and mdata['dc']['creators']:
                    version_info['first_author'] = mdata['dc']['creators'][0].get('creatorName', 'Unknown')
                
                # Get modification date from metadata
                if 'mdf' in mdata and 'ingest_date' in mdata['mdf']:
                    version_info['metadata_updated_at'] = mdata['mdf']['ingest_date']
            
            # Include metadata diff if requested and not the oldest version
            if include_diff and i < len(versions) - 1:
                current_metadata = version_data.get('dataset_mdata', {})
                previous_metadata = versions[i + 1].get('dataset_mdata', {})
                
                if current_metadata and previous_metadata:
                    diff = validator.generate_metadata_diff(previous_metadata, current_metadata)
                    version_info['changes'] = diff
                else:
                    version_info['changes'] = {}
            elif include_diff and i == len(versions) - 1:
                # First version - no previous version to compare against
                version_info['changes'] = {'note': 'Initial version'}
            
            version_list.append(version_info)
        
        response_data = {
            'success': True,
            'source_id': source_id,
            'versions': version_list,
            'total_versions': len(version_list),
            'include_diff': include_diff
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