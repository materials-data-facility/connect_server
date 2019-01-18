from mdf_connect_server.processor import Validator
import pytest


def test_validator():
    good_dataset1 = {
        "dc": {
            'creators': [{
                'creatorName': 'Footon, Bartholomew',
                'familyName': 'Footon',
                'givenName': 'Bartholomew'
            }],
            'publicationYear': '2018',
            'publisher': 'Materials Data Facility',
            'resourceType': {
                'resourceType': 'Dataset',
                'resourceTypeGeneral': 'Dataset'
            },
            'titles': [{
                'title': 'Foo Bar Dataset'
            }]
        },
        "mdf": {
            "source_name": "foo_bar_dataset",
            "source_id": "foo_bar_dataset_v1",
            "acl": ["public"]
        },
        "custom": {
            "foo": "bar"
        }
    }
    good_dataset2 = {
        "dc": {
            'creators': [{
                'creatorName': 'Footon, Bartholomew',
                'familyName': 'Footon',
                'givenName': 'Bartholomew'
            }],
            'publicationYear': '2018',
            'publisher': 'Materials Data Facility',
            'resourceType': {
                'resourceType': 'Dataset',
                'resourceTypeGeneral': 'Dataset'
            },
            'titles': [{
                'title': 'Foo Bar Dataset'
            }]
        },
        "mdf": {
            "source_name": "foo_bar_dataset",
            "source_id": "foo_bar_dataset_v1",
            "acl": ["public"],
            "version": 1,
            "repositories": ["MDF"]
        },
        "mrr": {
            "dataOrigin": ["experimental"]
        },
        "custom": {
            "foo": "bar"
        }
    }
    bad_dataset = {
        "invalid": True,
        "dc": {
            "qwerty": "asdf"
        }
    }
    good_record1 = {
        "mdf": {
            "source_name": "foo_bar_dataset",
            "source_id": "foo_bar_dataset_v1",
            "acl": ["public"]
        },
        "files": [{
            "data_type": "example",
            "filename": "foo.bar"
        }],
        "material": {
        }
    }
    good_record2 = {
        "mdf": {
            "source_name": "foo_bar_dataset",
            "source_id": "foo_bar_dataset_v1",
            "acl": ["public"]
        },
        "files": [{
            "data_type": "example",
            "filename": "foo.bar"
        }, {
            "data_type": "also-example",
            "globus": "globus://12345a/a/b/c/foo.txt",
            "url": "https://example.com/data/foo.txt",
            "filename": "foo.txt",
            "mime_type": "silent",
            "md5": "password12345"
        }],
        "material": {
            "composition": "FFO2"
        },
        "calphad": {
            "phases": [
                "waxing",
                "waning",
                "waiting"
            ]
        },
        "crystal_structure": {
            "space_group_number": 42,
            "number_of_atoms": 42,
            "volume": 42.42,
            "stoichiometry": "AB4C42D500"
        },
        "dft": {
            "converged": False,
            "exchange_correlation_functional": "yep",
            "cutoff_energy": 555.1234
        },
        "electron_microscopy": {
            "acquisition_mode": "on",
            "beam_energy": 9998,
            "detector": "lost",
            "magnification": -5
        },
        "image": {
            "width": 99,
            "height": 101,
            "megapixels": 0.00001
        },
        "custom": {
            "is_okay": "True"
        }
    }
    bad_record = {
        "notallowed": "yes",
        "mdf": {
            "pet": "mosquito"
        }
    }
    correct1 = [{
        'dc': {
            'creators': [{
                'creatorName': 'Footon, Bartholomew',
                'familyName': 'Footon',
                'givenName': 'Bartholomew'
            }],
            'publicationYear': '2018',
            'publisher': 'Materials Data Facility',
            'resourceType': {
                'resourceType': 'Dataset',
                'resourceTypeGeneral': 'Dataset'
            },
            'titles': [{
                'title': 'Foo Bar Dataset'
            }]
        },
        'mdf': {
            'source_name': 'foo_bar_dataset',
            'source_id': 'foo_bar_dataset_v1',
            'acl': ['public'],
            'scroll_id': 0,
            'resource_type': 'dataset',
            'version': 1
        },
        'custom': {
            'foo': 'bar'
        },
        'services': {},
        'data': {}
    }, {
        'mdf': {
            'source_name': 'foo_bar_dataset',
            'source_id': 'foo_bar_dataset_v1',
            'acl': ['public'],
            'scroll_id': 1,
            'resource_type': 'record',
            'version': 1
        },
        'files': [{
            'data_type': 'example',
            'filename': 'foo.bar'
        }],
        'material': {}
    }]
    correct2 = [{
        'dc': {
            'creators': [{
                'creatorName': 'Footon, Bartholomew',
                'familyName': 'Footon',
                'givenName': 'Bartholomew'
            }],
            'publicationYear': '2018',
            'publisher': 'Materials Data Facility',
            'resourceType': {
                'resourceType': 'Dataset',
                'resourceTypeGeneral': 'Dataset'
            },
            'titles': [{
                'title': 'Foo Bar Dataset'
            }]
        },
        'mdf': {
            'source_name': 'foo_bar_dataset',
            'source_id': 'foo_bar_dataset_v1',
            'acl': ['public'],
            'version': 1,
            'repositories': ['MDF'],
            'scroll_id': 0,
            'resource_type': 'dataset'
        },
        'mrr': {
            'dataOrigin': ['experimental']
        },
        'custom': {
            'foo': 'bar'
        },
        'services': {},
        'data': {}
    }, {
        'mdf': {
            'source_name': 'foo_bar_dataset',
            'source_id': 'foo_bar_dataset_v1',
            'acl': ['public'],
            'scroll_id': 1,
            'resource_type': 'record',
            'version': 1,
            'repositories': ['MDF']
        },
        'files': [{
            'data_type': 'example',
            'filename': 'foo.bar'
        }],
        'material': {}
    }, {
        'mdf': {
            'source_name': 'foo_bar_dataset',
            'source_id': 'foo_bar_dataset_v1',
            'acl': ['public'],
            'scroll_id': 3,
            'resource_type': 'record',
            'version': 1,
            'repositories': ['MDF']
        },
        'files': [{
            'data_type': 'example',
            'filename': 'foo.bar'
        }, {
            'data_type': 'also-example',
            'globus': 'globus://12345a/a/b/c/foo.txt',
            'url': 'https://example.com/data/foo.txt',
            'filename': 'foo.txt',
            'mime_type': 'silent',
            'md5': 'password12345'
        }],
        'material': {
            'composition': 'FFO2',
            'elements': ['F', 'O']
        },
        'calphad': {
            'phases': ['waxing', 'waning', 'waiting']
        },
        'crystal_structure': {
            'space_group_number': 42,
            'number_of_atoms': 42,
            'volume': 42.42,
            'stoichiometry': 'AB4C42D500'
        },
        'dft': {
            'converged': False,
            'exchange_correlation_functional': 'yep',
            'cutoff_energy': 555.1234
        },
        'electron_microscopy': {
            'acquisition_mode': 'on',
            'beam_energy': 9998,
            'detector': 'lost',
            'magnification': -5
        },
        'image': {
            'width': 99,
            'height': 101,
            'megapixels': 0.00001
        },
        'custom': {
            'is_okay': "True"
        }
    }]

    val = Validator()
    assert val.status() == "Dataset not started."
    assert val.add_record({}) == {
                            "success": False,
                            "error": "Dataset not started."
                            }
    with pytest.raises(ValueError):
        next(val.get_finished_dataset())
    # Regular operation
    assert val.start_dataset(good_dataset1)["success"]
    assert val.add_record(good_record1)["success"]
    res = list(val.get_finished_dataset())
    assert len(res) == 2
    # Must remove dynamic data before comparison
    assert res[0]["mdf"].pop("mdf_id")
    assert res[0]["mdf"].pop("ingest_date")
    assert res[1]["mdf"].pop("mdf_id")
    assert res[1]["mdf"].pop("parent_id")
    assert res[1]["mdf"].pop("ingest_date")
    assert res == correct1
    assert val.status() == "Dataset fully read out."

    # Exceptions, other dataset
    with pytest.raises(ValueError):
        next(val.get_finished_dataset())
    gd2_start_res = val.start_dataset(good_dataset2)
    assert gd2_start_res["success"], gd2_start_res["error"]
    assert val.status() == "Dataset started and still accepting records."
    assert val.start_dataset({}) == {
                                "success": False,
                                "error": "Dataset validation already in progress."
                                }
    assert val.add_record(good_record1)["success"]

    bad_res = val.add_record(bad_record)
    assert bad_res["success"] is False
    assert "Invalid metadata" in bad_res["error"]

    gr2_add_res = val.add_record(good_record2)
    assert gr2_add_res["success"], gr2_add_res["error"]
    res = list(val.get_finished_dataset())
    assert len(res) == 3
    # Must remove dynamic data before comparison
    assert res[0]["mdf"].pop("mdf_id")
    assert res[0]["mdf"].pop("ingest_date")
    assert res[1]["mdf"].pop("mdf_id")
    assert res[1]["mdf"].pop("parent_id")
    assert res[1]["mdf"].pop("ingest_date")
    assert res[2]["mdf"].pop("mdf_id")
    assert res[2]["mdf"].pop("parent_id")
    assert res[2]["mdf"].pop("ingest_date")
    assert res == correct2

    bad_res = val.add_record({})
    assert bad_res["success"] is False
    assert "Dataset has been finished" in bad_res["error"]

    bad_res = val.start_dataset(bad_dataset)
    assert bad_res["success"] is False
    assert "Invalid metadata" in bad_res["error"]
