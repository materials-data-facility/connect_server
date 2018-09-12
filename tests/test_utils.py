from mdf_connect_server.utils import fetch_whitelist, make_source_id
import pytest  # noqa: F401


def test_fetch_whitelist():
    # Assert admins are correct
    jgaff = "117e8833-68f5-4cb2-afb3-05b25db69be1"
    blaiszik = "c8745ef4-d274-11e5-bee8-3b6845397ac9"
    admin = fetch_whitelist("admin")
    assert jgaff in admin and blaiszik in admin
    # Each lower level should be a superset of the previous
    ingest = fetch_whitelist("ingest")
    assert len(ingest) > len(admin)
    assert all([x in ingest for x in admin])
    convert = fetch_whitelist("convert")
    assert len(convert) > len(ingest)
    assert all([x in convert for x in ingest])


def test_make_source_id():
    # Standard usage
    correct1 = {
        "source_id": "foo_bar_study_v1",
        "source_name": "foo_bar_study",
        "version": 1,
        "user_id_list": set()
    }
    assert make_source_id("Foo and Bar:,; a !@#$ Study", test=False) == correct1
    assert make_source_id("foo_bar_study_v1", test=False) == correct1

    # Test usage
    correct2 = {
        "source_id": "_test_foo_bar_study_v1",
        "source_name": "_test_foo_bar_study",
        "version": 1,
        "user_id_list": set()
    }
    assert make_source_id("Foo and Bar:,; a !@#$ Study", test=True) == correct2
    assert make_source_id("foo_bar_study_v1", test=True) == correct2

    # Double usage should not mutate
    assert make_source_id(correct1["source_id"], test=False) == correct1
    assert make_source_id(correct1["source_name"], test=False) == correct1
    assert make_source_id(correct2["source_id"], test=True) == correct2
    assert make_source_id(correct2["source_name"], test=True) == correct2

    # TODO: Set/find known static source_id in StatusDB
    # With previous versions
    # res = make_source_id("", test=False)
    # assert res["version"] > 1
    # assert res["source_name"] == ""
    # assert res["source_id"].endswith(str(res["version"]))
