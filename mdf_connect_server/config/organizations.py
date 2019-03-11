ORGANIZATIONS = [{
    # Sample Organization
    "canonical_name": "Sample Organization",
    "aliases": [
        "Test Organization",
        "Do Not Use"
    ],
    "description": "A sample organization for testing, and not for Production use.",
    "homepage": "https://materialsdatafacility.org",
    "permission_groups": [
        "5fc63928-3752-11e8-9c6f-0e00fd09bf20"  # MDF Connect Admins
    ],
    "acl": [
        "5fc63928-3752-11e8-9c6f-0e00fd09bf20"
    ],
    "data_locations": [
        ("globus://e38ee745-6d04-11e5-ba46-22000b92c6ec/MDF/mdf_connect/test_files/"
         "deleteme_contents/")
    ],
    "curation": True,
    "project_blocks": [],
    "required_fields": [
        "dc.descriptions.description"
    ],
    "parent_organizations": [],
    "integrations": []
}, {
    # NanoMFG
    "canonical_name": "NanoMFG",
    "description": "The Nanomanufacturing Group.",
    "homepage": "https://nanohub.org/groups/nanomfg",
    "permission_groups": [
        "TODO"  # TODO
    ],
    "project_blocks": [
        "nanomfg"
    ],
    "parent_organizations": [
        "National Science Foundation"
    ]
}, {
    # National Science Foundation
    "canonical_name": "National Science Foundation",
    "aliases": [
        "NSF"
    ],
    "description": "The National Science Foundation of the United States.",
    "homepage": "https://nsf.gov/",
    "permission_groups": [
        "public"
    ]
}]
