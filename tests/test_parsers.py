import json
import os

import mdf_connect_server.processor.transformer as parsers
import mdf_toolbox
import pytest  # noqa: F401


DATASET_PARAM = {
    "mdf": {
        "source_name": "test_dataset"
    }
}
BASE_PATH = os.path.join(os.path.dirname(__file__), "test_files")
NO_DATA_FILE = os.path.join(BASE_PATH, "no_data.dat")
NA_PATH = os.path.join(BASE_PATH, "does_not_exist.dat")


def test_crystal_structure():
    cif1_path = os.path.join(BASE_PATH, "crystal_structure", "Al2O3.cif")
    cif1_record = {
        'crystal_structure': {
            'number_of_atoms': 30,
            'space_group_number': 167,
            'stoichiometry': 'A2B3',
            'volume': 255.03329306417825
        },
        'material': {
            'composition': 'Al12O18'
        }
    }
    cif2_path = os.path.join(BASE_PATH, "crystal_structure", "C13H22O3.cif")
    cif2_record = {
        'crystal_structure': {
            'number_of_atoms': 152,
            'space_group_number': 2,
            'stoichiometry': 'A3B13C22',
            'volume': 1210.766292669533
        },
        'material': {
            'composition': 'H88C52O12'
        }
    }
    cif3_path = os.path.join(BASE_PATH, "crystal_structure", "Ce3VO16.cif")
    cif3_record = {
        'crystal_structure': {
            'number_of_atoms': 160,
            'space_group_number': 141,
            'stoichiometry': 'AB3C16',
            'volume': 355.624825696
        },
        'material': {
            'composition': 'Ce24V8O128'
        }
    }
    cif4_path = os.path.join(BASE_PATH, "crystal_structure", "diamond.cif")
    cif4_record = {
        'crystal_structure': {
            'number_of_atoms': 8,
            'space_group_number': 227,
            'stoichiometry': 'A',
            'volume': 45.64126284906666
        },
        'material': {
            'composition': 'C8'
        }
    }

    assert parsers.parse_crystal_structure([cif1_path]) == cif1_record
    assert parsers.parse_crystal_structure([cif2_path]) == cif2_record
    assert parsers.parse_crystal_structure([cif3_path]) == cif3_record
    assert parsers.parse_crystal_structure([cif4_path]) == cif4_record
    assert parsers.parse_crystal_structure([NO_DATA_FILE]) == {}
    assert parsers.parse_crystal_structure([NA_PATH]) == {}


def test_tdb():
    tdb1_path = os.path.join(BASE_PATH, "tdb", "PbSSeTe_Na.TDB")
    tdb1_record = {
        'calphad': {
            'phases': [
                'LIQUID',
                'FCC_A1',
                'HALITE',
                'HEXAGONAL_A8',
                'ORTHORHOMBIC_S',
                'BCC_A2',
                'NA2TE',
                'NATE',
                'NATE3',
                'NA2SE',
                'NASE',
                'NASE2',
                'NA2S',
                'NAS',
                'NAS2'
            ]
        },
        'material': {
            'composition': 'SeVaTeNaSPb'
        }
    }
    tdb2_path = os.path.join(BASE_PATH, "tdb", "test_AuSi.TDB")
    tdb2_record = {
        'calphad': {
            'phases': [
                'LIQUID',
                'BCC_A2',
                'CBCC_A12',
                'CUB_A13',
                'DIAMOND_A4',
                'FCC_A1',
                'HCP_A3',
                'HCP_ZN'
            ]
        },
        'material': {
            'composition': 'SiVaAu'
        }
    }
    tdb3_path = os.path.join(BASE_PATH, "tdb", "test_PbTe.TDB")
    tdb3_record = {
        'calphad': {
            'phases': [
                'LIQUID',
                'PBTE',
                'HEXAGONAL_A8',
                'RHOMBOHEDRAL_A7'
            ]
        },
        'material': {
            'composition': 'TeVaPb'
        }
    }

    assert mdf_toolbox.insensitive_comparison(parsers.parse_tdb([tdb1_path]), tdb1_record,
                                              string_insensitive=True)
    assert mdf_toolbox.insensitive_comparison(parsers.parse_tdb([tdb2_path]), tdb2_record,
                                              string_insensitive=True)
    assert mdf_toolbox.insensitive_comparison(parsers.parse_tdb([tdb3_path]), tdb3_record,
                                              string_insensitive=True)
    assert parsers.parse_tdb([NO_DATA_FILE]) == {}
    assert parsers.parse_tdb([NA_PATH]) == {}


def test_pif():
    # TODO
    pass


def test_json(tmpdir):
    json_data = {
        "dict1": {
            "field1": "value1",
            "field2": 2
        },
        "dict2": {
            "nested1": {
                "field1": True,
                "field3": "value3"
            }
        },
        "compost": "CN25"
    }
    json_file = tmpdir.join("test.json")
    with json_file.open(mode='w', ensure=True) as f:
        json.dump(json_data, f)
    group = [json_file.strpath]
    mapping1 = {
        "__custom": {
            "foo": "dict1.field1",
            "bar": "dict2.nested1.field1"
        },
        "material": {
            "composition": "compost"
        }
    }
    mapping2 = {
        "__custom.foo": "dict1.field1",
        "__custom.bar": "dict2.nested1.field1",
        "material.composition": "compost"
    }
    correct_record = {
        "material": {
            "composition": "CN25"
        },
        "test_dataset": {
            "foo": "value1",
            "bar": True
        }
    }

    # Test with proper mappings
    assert parsers.parse_json(group, params={
                                        "dataset": DATASET_PARAM,
                                        "parsers": {
                                            "json": {
                                                "mapping": mapping1
                                            }
                                        }
                                     }) == [correct_record]
    assert parsers.parse_json(group, params={
                                        "dataset": DATASET_PARAM,
                                        "parsers": {
                                            "json": {
                                                "mapping": mapping2
                                            }
                                        }
                                     }) == [correct_record]
    # Test failure modes
    assert parsers.parse_json(group, {}) == {}
    assert parsers.parse_json([], params={
                                    "dataset": DATASET_PARAM,
                                    "parsers": {
                                        "json": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == []
    assert parsers.parse_json([NO_DATA_FILE], params={
                                    "dataset": DATASET_PARAM,
                                    "parsers": {
                                        "json": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == {}
    assert parsers.parse_json([NA_PATH], params={
                                    "dataset": DATASET_PARAM,
                                    "parsers": {
                                        "json": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == {}


def test_csv():
    # TODO
    pass


def test_yaml():
    # TODO
    pass


def test_xml(tmpdir):
    xml_data = ('<?xml version="1.0" encoding="utf-8"?>\n<root><dict1><field1>value1</field1>'
                '<field2>2</field2></dict1><dict2><nested1><field1>true</field1>'
                '<field3>value3</field3></nested1></dict2><compost>CN25</compost></root>')
    xml_file = tmpdir.join("test.xml")
    with xml_file.open(mode='w', ensure=True) as f:
        f.write(xml_data)
    group = [xml_file.strpath]
    mapping1 = {
        "__custom": {
            "foo": "root.dict1.field1",
            "bar": "root.dict2.nested1.field1"
        },
        "material": {
            "composition": "root.compost"
        }
    }
    mapping2 = {
        "__custom.foo": "root.dict1.field1",
        "__custom.bar": "root.dict2.nested1.field1",
        "material.composition": "root.compost"
    }
    correct_record = {
        "material": {
            "composition": "CN25"
        },
        "test_dataset": {
            "foo": "value1",
            "bar": 'true'
        }
    }

    # Test with proper mappings
    assert parsers.parse_xml(group, params={
                                        "dataset": DATASET_PARAM,
                                        "parsers": {
                                            "xml": {
                                                "mapping": mapping1
                                            }
                                        }
                                     }) == [correct_record]
    assert parsers.parse_xml(group, params={
                                        "dataset": DATASET_PARAM,
                                        "parsers": {
                                            "xml": {
                                                "mapping": mapping2
                                            }
                                        }
                                     }) == [correct_record]
    # Test failure modes
    assert parsers.parse_xml(group, {}) == {}
    assert parsers.parse_xml([], params={
                                    "dataset": DATASET_PARAM,
                                    "parsers": {
                                        "xml": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == []
    assert parsers.parse_xml([NO_DATA_FILE], params={
                                    "dataset": DATASET_PARAM,
                                    "parsers": {
                                        "xml": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == {}
    assert parsers.parse_xml([NA_PATH], params={
                                    "dataset": DATASET_PARAM,
                                    "parsers": {
                                        "xml": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == {}


def test_excel():
    # TODO
    pass


def test_image():
    # TODO
    pass


def test_electron_microscopy():
    # TODO
    pass


def test_filename():
    mapping = {
        "material.composition": "^.{2}",  # First two chars are always composition
        "__custom.foo": "foo:.{3}",  # 3 chars after foo is foo
        "__custom.ext": "\\..{3,4}$"  # 3 or 4 char extension
    }
    group = ["He_abcdeffoo:FOO.txt", "Al123Smith_et_al.and_co.data", "O2foo:bar"]
    correct = [{
        'test_dataset': {
            'ext': '.txt',
            'foo': 'foo:FOO'
        },
        'material': {
            'composition': 'He'
        }
    }, {
        'test_dataset': {
            'ext': '.data'
        },
        'material': {
            'composition': 'Al'
        }
    }, {
        'test_dataset': {
            'foo': 'foo:bar'
        },
        'material': {
            'composition': 'O2'
        }
    }]
    assert parsers.parse_filename(group, params={
                                            "dataset": DATASET_PARAM,
                                            "parsers": {
                                                "filename": {
                                                    "mapping": mapping
                                                }
                                            }
                                         }) == correct
    # Failures
    assert parsers.parse_filename(group, params={}) == {}
    assert parsers.parse_filename([], params={
                                            "dataset": DATASET_PARAM,
                                            "parsers": {
                                                "filename": {
                                                    "mapping": mapping
                                                }
                                            }
                                         }) == []


def test_file_info():
    # TODO
    pass
