GROUPINGS = {
    "known_formats": {
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
    }
}
