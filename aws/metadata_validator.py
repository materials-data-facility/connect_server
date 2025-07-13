import json
import os
import logging
from datetime import datetime
from copy import deepcopy

import jsonschema

logger = logging.getLogger(__name__)


class MetadataValidator:
    """Validator for metadata updates with field-level permissions and validation."""
    
    # Fields that can be updated by users (not admin-only)
    USER_UPDATABLE_FIELDS = {
        'dc.titles': 'titles',
        'dc.creators': 'authors', 
        'dc.descriptions': 'description',
        'dc.subjects': 'tags'
    }
    
    # Fields that are admin-only or system-managed
    RESTRICTED_FIELDS = {
        'mdf.source_id',
        'mdf.versioned_source_id', 
        'mdf.version',
        'mdf.resource_type',
        'mdf.data_contact',
        'mdf.data_contributor',
        'mdf.ingest_date',
        'datacite.doi',
        'datacite.resourcetype',
        'datacite.resourcetypegeneral',
        'datacite.publicationyear',
        'services'
    }

    def __init__(self):
        self.schema_path = os.environ.get('SCHEMA_PATH', "./schemas/schemas")
        self._load_schema()

    def _load_schema(self):
        """Load the JSON schema for validation."""
        try:
            with open(os.path.join(self.schema_path, "connect_submission.json")) as schema_file:
                self.schema = json.load(schema_file)
                self.resolver = jsonschema.RefResolver(
                    base_uri=f"file://{os.getcwd()}/{self.schema_path}/",
                    referrer=self.schema
                )
        except Exception as e:
            logger.error(f"Failed to load schema: {e}")
            raise

    def validate_field_permissions(self, field_updates, user_role='user'):
        """Validate that user has permission to update the specified fields.
        
        Arguments:
        field_updates (dict): Dictionary of field paths to new values
        user_role (str): Role of the user ('user' or 'admin')
        
        Returns:
        dict: Validation result with success flag and any errors
        """
        errors = []
        
        for field_path in field_updates.keys():
            # Check if field is in restricted list
            if field_path in self.RESTRICTED_FIELDS and user_role != 'admin':
                errors.append(f"Field '{field_path}' can only be updated by administrators")
                continue
                
            # Check if field is user-updatable
            if field_path not in self.USER_UPDATABLE_FIELDS and user_role == 'user':
                errors.append(f"Field '{field_path}' is not allowed for user updates")
        
        return {
            'success': len(errors) == 0,
            'errors': errors
        }

    def validate_field_values(self, field_updates):
        """Validate the values for specific metadata fields.
        
        Arguments:
        field_updates (dict): Dictionary of field paths to new values
        
        Returns:
        dict: Validation result with success flag and any errors
        """
        errors = []
        
        for field_path, value in field_updates.items():
            if field_path == 'dc.titles':
                errors.extend(self._validate_titles(value))
            elif field_path == 'dc.creators':
                errors.extend(self._validate_creators(value))
            elif field_path == 'dc.descriptions':
                errors.extend(self._validate_descriptions(value))
            elif field_path == 'dc.subjects':
                errors.extend(self._validate_subjects(value))
        
        return {
            'success': len(errors) == 0,
            'errors': errors
        }

    def _validate_titles(self, titles):
        """Validate titles field."""
        errors = []
        
        if not isinstance(titles, list):
            errors.append("Titles must be a list")
            return errors
            
        if len(titles) == 0:
            errors.append("At least one title is required")
            return errors
            
        for i, title in enumerate(titles):
            if not isinstance(title, dict):
                errors.append(f"Title {i} must be an object")
                continue
                
            if 'title' not in title:
                errors.append(f"Title {i} must have a 'title' field")
                
            if not isinstance(title.get('title'), str) or not title.get('title').strip():
                errors.append(f"Title {i} 'title' field must be a non-empty string")
        
        return errors

    def _validate_creators(self, creators):
        """Validate creators/authors field."""
        errors = []
        
        if not isinstance(creators, list):
            errors.append("Creators must be a list")
            return errors
            
        if len(creators) == 0:
            errors.append("At least one creator is required")
            return errors
            
        for i, creator in enumerate(creators):
            if not isinstance(creator, dict):
                errors.append(f"Creator {i} must be an object")
                continue
                
            if 'creatorName' not in creator:
                errors.append(f"Creator {i} must have a 'creatorName' field")
                
            if not isinstance(creator.get('creatorName'), str) or not creator.get('creatorName').strip():
                errors.append(f"Creator {i} 'creatorName' must be a non-empty string")
        
        return errors

    def _validate_descriptions(self, descriptions):
        """Validate descriptions field."""
        errors = []
        
        if not isinstance(descriptions, list):
            errors.append("Descriptions must be a list")
            return errors
            
        for i, description in enumerate(descriptions):
            if not isinstance(description, dict):
                errors.append(f"Description {i} must be an object")
                continue
                
            if 'description' not in description:
                errors.append(f"Description {i} must have a 'description' field")
                
            if not isinstance(description.get('description'), str) or not description.get('description').strip():
                errors.append(f"Description {i} 'description' field must be a non-empty string")
        
        return errors

    def _validate_subjects(self, subjects):
        """Validate subjects/tags field."""
        errors = []
        
        if not isinstance(subjects, list):
            errors.append("Subjects must be a list")
            return errors
            
        for i, subject in enumerate(subjects):
            if not isinstance(subject, str) or not subject.strip():
                errors.append(f"Subject {i} must be a non-empty string")
        
        return errors

    def apply_metadata_updates(self, original_metadata, field_updates):
        """Apply field updates to existing metadata.
        
        Arguments:
        original_metadata (dict): The original metadata object
        field_updates (dict): Dictionary of field paths to new values
        
        Returns:
        dict: Updated metadata object
        """
        updated_metadata = deepcopy(original_metadata)
        
        for field_path, new_value in field_updates.items():
            # Navigate to the correct nested location
            parts = field_path.split('.')
            current = updated_metadata
            
            # Navigate to parent object
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # Set the new value
            current[parts[-1]] = new_value
        
        # Update the modification timestamp
        updated_metadata['mdf']['ingest_date'] = datetime.utcnow().isoformat() + "Z"
        
        return updated_metadata

    def validate_complete_metadata(self, metadata):
        """Validate the complete metadata object against the schema.
        
        Arguments:
        metadata (dict): Complete metadata object to validate
        
        Returns:
        dict: Validation result with success flag and any errors
        """
        try:
            jsonschema.validate(metadata, self.schema, resolver=self.resolver)
            return {'success': True, 'errors': []}
        except jsonschema.ValidationError as e:
            return {
                'success': False,
                'errors': [f"Schema validation failed: {str(e).split(chr(10))[0]}"]
            }

    def generate_metadata_diff(self, original_metadata, updated_metadata):
        """Generate a diff showing what changed between versions.
        
        Arguments:
        original_metadata (dict): Original metadata
        updated_metadata (dict): Updated metadata
        
        Returns:
        dict: Diff showing changes
        """
        changes = {}
        
        for field_path in self.USER_UPDATABLE_FIELDS.keys():
            original_value = self._get_nested_value(original_metadata, field_path)
            updated_value = self._get_nested_value(updated_metadata, field_path)
            
            if original_value != updated_value:
                changes[field_path] = {
                    'old': original_value,
                    'new': updated_value
                }
        
        return changes

    def _get_nested_value(self, data, field_path):
        """Get a nested value from a dictionary using dot notation."""
        parts = field_path.split('.')
        current = data
        
        try:
            for part in parts:
                current = current[part]
            return current
        except (KeyError, TypeError):
            return None