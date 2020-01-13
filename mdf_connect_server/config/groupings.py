GROUPINGS = {
    "known_formats": {
        "cif": {
            "files": [
                ".cif",
            ],
            "extractors": [
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
            "extractors": [
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
            "extractors": [
                "json"
            ],
            "params": {}
        },
        "csv": {
            "files": [
                ".csv"
            ],
            "extractors": [
                "csv",
                "pif"
            ],
            "params": {
                "include": [
                    "template_csv"
                ]
            }
        }
    }
}
