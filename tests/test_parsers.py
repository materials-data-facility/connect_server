import json
import os

import mdf_connect
import pytest


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
    assert mdf_connect.transformer.parse_json(group, params={
                                                        "dataset": dataset_param,
                                                        "parsers": {
                                                            "json": {
                                                                "mapping": mapping1
                                                            }
                                                        }
                                                     }) == [correct_record]
    assert mdf_connect.transformer.parse_json(group, params={
                                                        "dataset": dataset_param,
                                                        "parsers": {
                                                            "json": {
                                                                "mapping": mapping2
                                                            }
                                                        }
                                                     }) == [correct_record]
    # Test failure modes
    assert mdf_connect.transformer.parse_json(group, {}) == {}
    assert mdf_connect.transformer.parse_json([], params={
                                                        "dataset": dataset_param,
                                                        "parsers": {
                                                            "json": {
                                                                "mapping": mapping2
                                                            }
                                                        }
                                                  }) == []

