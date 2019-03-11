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
    # Advanced Photon Source
    "canonical_name": "Advanced Photon Source",
    "aliases": [
        "APS"
    ],
    "description": ("The Advanced Photon Source is an Office of Science User Facility "
                    "operated for the U.S. Department of Energy Office of Science by "
                    "Argonne National Laboratory"),
    "homepage": "https://www.aps.anl.gov/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "Argonne National Laboratory"
    ]
}, {
    # APS Sector 1
    "canonical_name": "APS Sector 1",
    "aliases": [
    ],
    "description": "Sector 1 of the Advanced Photon Source.",
    "homepage": ("https://www.aps.anl.gov/Users-Information/Help-Reference/"
                 "Contacts/Sector-Beamline-Locations-Phones"),
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "Advanced Photon Source"
    ]
}, {
    # APS Sector 2
    "canonical_name": "APS Sector 2",
    "aliases": [
    ],
    "description": "Sector 2 of the Advanced Photon Source.",
    "homepage": ("https://www.aps.anl.gov/Users-Information/Help-Reference/"
                 "Contacts/Sector-Beamline-Locations-Phones"),
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "Advanced Photon Source"
    ]
}, {
    # Argonne National Laboratory
    "canonical_name": "Argonne National Laboratory",
    "aliases": [
        "ANL"
    ],
    "description": ("Argonne serves America as a science and energy laboratory "
                    "distinguished by the breadth of our R&D capabilities in concert "
                    "with our powerful suite of experimental and computational facilities."),
    "homepage": "https://www.anl.gov/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "U.S. Department of Energy"
    ]
}, {
    # Center for PRISMS
    "canonical_name": "Center for Predictive Integrated Structural Materials Science",
    "aliases": [
        "PRISMS",
        ("Department of Energy Software Innovation Center for Integrated "
         "Multi-Scale Modeling of Structural Metals")
    ],
    "description": ("Combining the efforts of experimental and computational researchers, "
                    "the overarching goal of the PRISMS Center is to establish a unique "
                    "scientific platform that will enable accelerated predictive materials "
                    "science for structural metals."),
    "homepage": "http://prisms-center.org/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "U.S. Department of Energy"
    ]
}, {
    # HTEM
    "canonical_name": "High Throughput Experimental Materials Database",
    "aliases": [
        "HTEM"
    ],
    "description": ("The HTEM DB contains information about materials obtained from "
                    "high-throughput experiments at NREL."),
    "homepage": "https://htem.nrel.gov/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "National Renewable Energy Laboratory"
    ]
}, {
    # Materials Commons
    "canonical_name": "Materials Commons",
    "aliases": [
        "MCPub"
    ],
    "description": ("The Materials Commons is a platform for organizing, collaborating, "
                    "publishing and sharing research data."),
    "homepage": "https://materialscommons.org/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "Center for Predictive Integrated Structural Materials Science"
    ]
}, {
    # NanoMFG
    "canonical_name": "NanoMFG",
    "description": ("The aim of the nanomanufacturing (nanoMFG) node is to develop "
                    "computational software tools aimed at creating smart, model-driven "
                    "and experimentally informed nanomanufactured structures and devices."),
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
    # National Institute of Standards and Technology
    "canonical_name": "National Institute of Standards and Technology",
    "aliases": [
        "NIST"
    ],
    "description": ("The National Institute of Standards and Technology (NIST) was founded "
                    "in 1901 and is now part of the U.S. Department of Commerce. NIST is "
                    "one of the nation's oldest physical science laboratories."),
    "homepage": "https://www.nist.gov/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "U.S. Department of Commerce"
    ]
}, {
    # National Renewable Energy Laboratory
    "canonical_name": "National Renewable Energy Laboratory",
    "aliases": [
        "NREL"
    ],
    "description": ("The National Renewable Energy Laboratory is a national laboratory "
                    "of the U.S. Department of Energy, Office of Energy Efficiency and "
                    "Renewable Energy, operated by the Alliance for Sustainable Energy, LLC."),
    "homepage": "https://www.nrel.gov/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "U.S. Department of Energy"
    ]
}, {
    # National Science Foundation
    "canonical_name": "National Science Foundation",
    "aliases": [
        "NSF"
    ],
    "description": ("The National Science Foundation (NSF) is an independent federal "
                    "agency created by Congress in 1950 \"to promote the progress "
                    "of science; to advance the national health, prosperity, and "
                    "welfare; to secure the national defense...\""),
    "homepage": "https://nsf.gov/",
    "permission_groups": [
        "public"
    ]
}, {
    # NIST Materials Data Repository
    "canonical_name": "NIST Materials Data Repository",
    "aliases": [
        "NIST MDR",
        "MDR"
    ],
    "description": ("The National Institute of Standards and Technology has created a "
                    "materials science data repository as part of an effort in "
                    "coordination with the Materials Genome Initiative (MGI) to "
                    "establish data exchange protocols and mechanisms that will foster "
                    "data sharing and reuse across a wide community of researchers, with "
                    "the goal of enhancing the quality of materials data and models."),
    "homepage": "https://materialsdata.nist.gov/",
    "permission_groups": [
        "public"
    ],
    "parent_organizations": [
        "National Institute of Standards and Technology"
    ]
}, {
    # U.S. Department of Commerce
    "canonical_name": "U.S. Department of Commerce",
    "description": ("The Department of Commerce promotes job creation and economic "
                    "growth by ensuring fair and reciprocal trade, providing the data "
                    "necessary to support commerce and constitutional democracy, and "
                    "fostering innovation by setting standards and conducting foundational "
                    "research and development."),
    "homepage": "https://www.commerce.gov/",
    "permission_groups": [
        "public"
    ]
}, {
    # U.S. Department of Energy
    "canonical_name": "U.S. Department of Energy",
    "aliases": [
        "DOE"
    ],
    "description": ("The mission of the Energy Department is to ensure America's "
                    "security and prosperity by addressing its energy, environmental "
                    "and nuclear challenges through transformative science and "
                    "technology solutions."),
    "homepage": "https://www.energy.gov/",
    "permission_groups": [
        "public"
    ]
}]
