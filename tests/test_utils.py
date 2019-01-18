from mdf_connect_server import utils
import pytest  # noqa: F401


def test_fetch_whitelist():
    # Assert admins are correct
    jgaff = "117e8833-68f5-4cb2-afb3-05b25db69be1"
    blaiszik = "c8745ef4-d274-11e5-bee8-3b6845397ac9"
    admin = utils.fetch_whitelist("admin")
    assert jgaff in admin and blaiszik in admin
    # Each lower level should be a superset of the previous
    ingest = utils.fetch_whitelist("ingest")
    assert len(ingest) > len(admin)
    assert all([x in ingest for x in admin])
    convert = utils.fetch_whitelist("convert")
    assert len(convert) > len(ingest)
    assert all([x in convert for x in ingest])


def test_make_source_id():
    # Standard usage
    correct1 = {
        "source_id": "foo_bar_v123_study_v1-1",
        "source_name": "foo_bar_v123_study",
        "search_version": 1,
        "submission_version": 1,
        "user_id_list": set()
    }
    assert utils.make_source_id("Foo and Bar:,; a V123 !@#$ Study", test=False) == correct1
    assert utils.make_source_id("foo_bar_v123_study_v1", test=False) == correct1
    assert utils.make_source_id("foo_bar_v123_study_v1-1", test=False) == correct1

    # Test usage
    correct2 = {
        "source_id": "_test_foo_bar_v123_study_v1-1",
        "source_name": "_test_foo_bar_v123_study",
        "search_version": 1,
        "submission_version": 1,
        "user_id_list": set()
    }
    assert utils.make_source_id("Foo and Bar:,; a V123 !@#$ Study", test=True) == correct2
    assert utils.make_source_id("foo_bar_v123_study_v1", test=True) == correct2
    assert utils.make_source_id("foo_bar_v123_study_v1-1", test=True) == correct2

    # Double usage should not mutate
    assert utils.make_source_id(correct1["source_id"], test=False) == correct1
    assert utils.make_source_id(correct1["source_name"], test=False) == correct1
    assert utils.make_source_id(correct2["source_id"], test=True) == correct2
    assert utils.make_source_id(correct2["source_name"], test=True) == correct2

    # TODO: Set/find known static source_id in StatusDB
    # With previous versions
    # res = utils.make_source_id("", test=False)
    # assert res["version"] > 1
    # assert res["source_name"] == ""
    # assert res["source_id"].endswith(str(res["version"]))


def test_split_source_id():
    # Standard form
    assert utils.split_source_id("_test_foo_bar_study_v1-1") == {
        "success": True,
        "source_name": "_test_foo_bar_study",
        "search_version": 1,
        "submission_version": 1
    }
    assert utils.split_source_id("study_v8_engines_v2-8") == {
        "success": True,
        "source_name": "study_v8_engines",
        "search_version": 2,
        "submission_version": 8
    }
    # Incorrect form
    assert utils.split_source_id("just_this") == {
        "success": False,
        "source_name": "just_this",
        "search_version": 0,
        "submission_version": 0
    }

    # TODO: Remove legacy-form support
    # Legacy form
    assert utils.split_source_id("_test_old_oqmd_v13") == {
        "success": True,
        "source_name": "_test_old_oqmd",
        "search_version": 13,
        "submission_version": 13
    }
    assert utils.split_source_id("ser_v1_ng_stuff_v2") == {
        "success": True,
        "source_name": "ser_v1_ng_stuff",
        "search_version": 2,
        "submission_version": 2
    }


def test_scan_status():
    # Regular usage
    # Requires multiple statuses in DB
    # TODO: Set/find known static source_id in StatusDB
    res = utils.scan_status()
    assert res["success"]
    count1 = len(res["results"])
    sample1 = res["results"][0]

    res = utils.scan_status(fields="source_id")
    assert res["success"]
    # Fields arg should not restrict results
    assert len(res["results"]) == count1
    # Only 'source_id' should be in results
    assert all([("source_id" in entry.keys() and len(entry.keys()) == 1)
                for entry in res["results"]])

    res = utils.scan_status(fields=["source_id", "test"])
    assert res["success"]
    assert len(res["results"]) == count1
    assert all([("source_id" in entry.keys() and "test" in entry.keys() and len(entry.keys()) == 2)
                for entry in res["results"]])

    res = utils.scan_status(filters=("submission_code", "!=", None))  # Exists
    assert res["success"]
    assert len(res["results"]) == count1

    res = utils.scan_status(filters=[("source_id", "!=", sample1["source_id"])])
    assert res["success"]
    assert len(res["results"]) == count1 - 1

    res = utils.scan_status(filters=[("source_id", "==", sample1["source_id"])])
    assert res["success"]
    assert len(res["results"]) == 1
    assert res["results"][0] == sample1

    res = utils.scan_status(filters=("submission_time", ">", sample1["submission_time"]))
    assert res["success"]
    count2 = len(res["results"])
    assert count2 < count1

    res = utils.scan_status(filters=[("submission_time", ">", sample1["submission_time"]),
                                     ("code", "<", sample1["code"])])
    assert res["success"]
    count3 = len(res["results"])
    assert count3 < count2

    # Errors
    res = utils.scan_status(fields=True)
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_status(filters=("field", "[]", "ab"))
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_status(filters=("field", "in", "ab"))
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_status(filters=("field", "@", "ab"))
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_status(filters={"field": "val"})
