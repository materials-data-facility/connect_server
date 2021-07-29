import json
import os
from collections import namedtuple

import jsonschema


class OrganizationException(Exception):
    pass


class OrganizationDatabaseError(jsonschema.ValidationError):
    pass


class Organization:
    def __init__(self, json_doc):
        self.json_doc = json_doc
        pass

    @classmethod
    def from_json_doc(cls, json_doc):
        object_name = namedtuple("Organization", json_doc.keys())(*json_doc.values())
        return object_name

    @classmethod
    def from_schema_repo(cls, canonical_name):
        schema_path = "./schemas/schemas"

        with open(os.path.join(schema_path, "..", "connect_aux_data", "organizations.json")) as org_schema:
            orgs = json.load(org_schema)
            filtered_orgs = list(filter(lambda org: org['canonical_name'] == canonical_name, orgs))
            if not filtered_orgs:
                raise OrganizationException(f"Organization {canonical_name} not found")

            if len(filtered_orgs) > 1:
                raise OrganizationException("Organization database contains duplicates")
            o = filtered_orgs[0]

        with open(os.path.join(schema_path, "organization.json")) as schema_file:
            schema = json.load(schema_file)
            resolver = jsonschema.RefResolver(base_uri="file://{}/{}/".format(os.getcwd(),
                                                                              schema_path),
                                              referrer=schema)
            try:
                jsonschema.validate(o, schema, resolver=resolver)
                object_name = namedtuple("Organization", o.keys())(*o.values())

                return object_name

            except jsonschema.ValidationError as e:
                print("Error in the organization json document")
                raise OrganizationDatabaseError(e)
