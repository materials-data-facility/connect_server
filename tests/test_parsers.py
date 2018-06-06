import json

import mdf_connect.transformer as parsers
import pytest  # noqa: F401


dataset_param = {
    "mdf": {
        "source_name": "test_dataset"
    }
}


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
                                        "dataset": dataset_param,
                                        "parsers": {
                                            "json": {
                                                "mapping": mapping1
                                            }
                                        }
                                     }) == [correct_record]
    assert parsers.parse_json(group, params={
                                        "dataset": dataset_param,
                                        "parsers": {
                                            "json": {
                                                "mapping": mapping2
                                            }
                                        }
                                     }) == [correct_record]
    # Test failure modes
    assert parsers.parse_json(group, {}) == {}
    assert parsers.parse_json([], params={
                                    "dataset": dataset_param,
                                    "parsers": {
                                        "json": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == []
    assert parsers.parse_json(["doesn't_exist.nope"], params={
                                    "dataset": dataset_param,
                                    "parsers": {
                                        "json": {
                                            "mapping": mapping2
                                        }
                                    }
                                  }) == {}


def test_filename():
    mapping = {
        "material.composition": "^.{2}",  # First two chars are always composition
        "__custom.foo": "foo:.{3}",  # 3 chars after foo is foo
        "__custom.ext": "\..{3,4}$"  # 3 or 4 char extension
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
                                            "dataset": dataset_param,
                                            "parsers": {
                                                "filename": {
                                                    "mapping": mapping
                                                }
                                            }
                                         }) == correct
    # Failures
    assert parsers.parse_filename(group, params={}) == {}
    assert parsers.parse_filename([], params={
                                            "dataset": dataset_param,
                                            "parsers": {
                                                "filename": {
                                                    "mapping": mapping
                                                }
                                            }
                                         }) == []
