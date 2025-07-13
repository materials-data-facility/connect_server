import json
import logging
import os
from urllib.parse import parse_qs

from dynamo_manager import DynamoManager

logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    """
    Handle GET /datasets requests to list user's datasets.
    
    Query Parameters:
    - limit: Number of results to return (default 20, max 100)
    - cursor: Pagination cursor for next page
    - status: Filter by status (active, cancelled, etc.)
    - organization: Filter by organization (admin only)
    
    Returns paginated list of user's datasets with metadata summaries.
    """
    print(json.dumps(event))
    
    # Extract user information from authorizer
    user_id = event['requestContext']['authorizer']['user_id']
    user_email = event['requestContext']['authorizer']['user_email']
    user_groups = eval(event['requestContext']['authorizer']['group_info'])
    identities = eval(event['requestContext']['authorizer']['identities'])
    
    # Parse query parameters
    query_params = event.get('queryStringParameters') or {}
    limit = min(int(query_params.get('limit', 20)), 100)  # Cap at 100
    cursor = query_params.get('cursor')
    status_filter = query_params.get('status')
    organization_filter = query_params.get('organization')
    
    # Parse cursor if provided
    last_key = None
    if cursor:
        try:
            import base64
            last_key = json.loads(base64.b64decode(cursor).decode('utf-8'))
        except Exception as e:
            logger.warning(f"Invalid cursor: {e}")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'success': False,
                    'error': 'Invalid pagination cursor'
                })
            }
    
    dynamo_manager = DynamoManager()
    
    try:
        # Query user's datasets using GSI for efficient retrieval
        result = dynamo_manager.query_user_datasets(
            user_id=user_id,
            limit=limit,
            last_key=last_key,
            status_filter=status_filter
        )
        
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
        
        # Process results to create summary view
        datasets = []
        for item in result['results']:
            # Extract metadata summary
            dataset_summary = {
                'source_id': item['source_id'],
                'version': item['version'],
                'status': item.get('status', 'unknown'),
                'updated_at': item.get('updated_at'),
                'created_at': item.get('created_at'),
                'organization': item.get('organization'),
                'user_id': item.get('user_id')
            }
            
            # Extract key metadata fields if available
            if 'dataset_mdata' in item:
                mdata = item['dataset_mdata']
                
                # Get title
                if 'dc' in mdata and 'titles' in mdata['dc'] and mdata['dc']['titles']:
                    dataset_summary['title'] = mdata['dc']['titles'][0].get('title', 'Untitled')
                else:
                    dataset_summary['title'] = 'Untitled'
                
                # Get first author
                if 'dc' in mdata and 'creators' in mdata['dc'] and mdata['dc']['creators']:
                    dataset_summary['first_author'] = mdata['dc']['creators'][0].get('creatorName', 'Unknown')
                else:
                    dataset_summary['first_author'] = 'Unknown'
                
                # Get description preview (first 200 chars)
                if 'dc' in mdata and 'descriptions' in mdata['dc'] and mdata['dc']['descriptions']:
                    desc = mdata['dc']['descriptions'][0].get('description', '')
                    dataset_summary['description_preview'] = desc[:200] + ('...' if len(desc) > 200 else '')
                else:
                    dataset_summary['description_preview'] = ''
                
                # Get tags/subjects
                if 'dc' in mdata and 'subjects' in mdata['dc']:
                    dataset_summary['tags'] = mdata['dc']['subjects'][:5]  # First 5 tags
                else:
                    dataset_summary['tags'] = []
                
                # Get resource type
                if 'mdf' in mdata and 'resource_type' in mdata['mdf']:
                    dataset_summary['resource_type'] = mdata['mdf']['resource_type']
                
                # Get DOI if available
                if 'datacite' in mdata and 'doi' in mdata['datacite']:
                    dataset_summary['doi'] = mdata['datacite']['doi']
            
            datasets.append(dataset_summary)
        
        # Create next page cursor if there are more results
        next_cursor = None
        if result.get('last_key'):
            import base64
            next_cursor = base64.b64encode(
                json.dumps(result['last_key']).encode('utf-8')
            ).decode('utf-8')
        
        response_body = {
            'success': True,
            'datasets': datasets,
            'count': len(datasets),
            'has_more': next_cursor is not None,
            'next_cursor': next_cursor
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response_body)
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