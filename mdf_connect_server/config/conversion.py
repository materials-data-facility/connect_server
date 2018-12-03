CONVERSION = {
    "GROUPING_RULES": {
        "cif": {
            "files": [
                ".cif",
            ],
            "parsers": [
                "crystal_structure",
                "pif"
            ],
            "params": {
                "include": [
                    "cif",
                    "dft"
                ]
            }
        },
        "vasp": {
            "files": [
                "outcar",
                "incar",
                "chgcar",
                "wavecar",
                "wavcar",
                "ozicar",
                "ibzcar",
                "kpoints",
                "doscar",
                "poscar",
                "contcar",
                "vasp_run.xml",
                "xdatcar"
            ],
            "parsers": [
                "pif",
                "crystal_structure"
            ],
            "params": {
                "include": [
                    "dft"
                ]
            }
        },
        "json": {
            "files": [
                ".json"
            ],
            "parsers": [
                "json"
            ],
            "params": {}
        },
        "csv": {
            "files": [
                ".csv"
            ],
            "parsers": [
                "csv",
                "pif"
            ],
            "params": {
                "include": [
                    "template_csv"
                ]
            }
        }
    },
    "REPOSITORY_RULES": {
        "APS Sector 1": {
            "aliases": [
            ],
            "parent_tags": [
                "Advanced Photon Source"
            ]
        },
        "APS Sector 2": {
            "aliases": [
            ],
            "parent_tags": [
                "Advanced Photon Source"
            ]
        },
        "Advanced Photon Source": {
            "aliases": [
                "APS"
            ],
            "parent_tags": [
                "Argonne National Laboratory"
            ]
        },
        "Argonne National Laboratory": {
            "aliases": [
                "ANL"
            ],
            "parent_tags": [
                "U.S. Department of Energy"
            ]
        },
        "High Throughput Experimental Materials Database": {
            "aliases": [
                "HTEM DB",
                "HTEM"
            ],
            "parent_tags": [
                "National Renewable Energy Laboratory"
            ]
        },
        "National Renewable Energy Laboratory": {
            "aliases": [
                "NREL"
            ],
            "parent_tags": [
                "U.S. Department of Energy"
            ]
        },
        "U.S. Department of Energy": {
            "aliases": [
                "DOE"
            ],
            "parent_tags": [
            ]
        },
        "Materials Commons": {
            "aliases": [
                "MCPub"
            ],
            "parent_tags": [
                "Center for Predictive Integrated Structural Materials Science"
            ]
        },
        "Center for Predictive Integrated Structural Materials Science": {
            "aliases": [
                "PRISMS",
                ("Department of Energy Software Innovation Center for Integrated "
                 "Multi-Scale Modeling of Structural Metals")
            ],
            "parent_tags": [
                "U.S. Department of Energy"
            ]
        },
        "NIST Materials Data Repository": {
            "aliases": [
                "NIST MDR", "MDR"
            ],
            "parent_tags": [
                "National Institute of Standards and Technology"
            ]
        },
        "National Institute of Standards and Technology": {
            "aliases": [
                "NIST"
            ],
            "parent_tags": [
                "U.S. Department of Commerce"
            ]
        },
        "U.S. Department of Commerce": {
            "aliases": [
                "DOC"
            ],
            "parent_tags": []
        }
    }
}
