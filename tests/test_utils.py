from mdf_connect_server import utils
import pytest  # noqa: F401


def test_fetch_whitelist():
    # Assert admins are correct
    jgaff = "117e8833-68f5-4cb2-afb3-05b25db69be1"
    blaiszik = "c8745ef4-d274-11e5-bee8-3b6845397ac9"
    admin = utils.fetch_whitelist("admin")
    assert jgaff in admin and blaiszik in admin

    convert = utils.fetch_whitelist("convert")
    assert len(convert) > len(admin)
    assert all([x in convert for x in admin])


def test_make_source_id():
    # Standard usage
    correct1 = {
        "source_id": "smith_foo_bar_stuff_v1.1",
        "source_name": "smith_foo_bar_stuff",
        "search_version": 1,
        "submission_version": 1,
        "user_id_list": set()
    }
    assert utils.make_source_id("Foo and Bar:,; a V123 !@#$ Stuff with dataset", "Smith",
                                test=False) == correct1
    assert utils.make_source_id("foo_bar_v123_stuff_v1", "Smith!", test=False) == correct1
    assert utils.make_source_id("foo_bar_v123_stuff_v1.1", "  smith   ", test=False) == correct1

    # Test usage
    correct2 = {
        "source_id": "_test_foxhound_foo_v123_thing_v1.1",
        "source_name": "_test_foxhound_foo_v123_thing",
        "search_version": 1,
        "submission_version": 1,
        "user_id_list": set()
    }
    assert utils.make_source_id("Foo and V123:,; a Bar !@#$ Thing", "Fox-Hound",
                                test=True) == correct2
    assert utils.make_source_id("foo_v123_bar_thing_v1", "Fox Hound", test=True) == correct2
    assert utils.make_source_id("foo_v123_bar_thing_v1-1", "Fox-!-Hound", test=True) == correct2

    # Low-token-count usage
    correct3 = {
        "source_id": "very_small_v1.1",
        "source_name": "very_small",
        "search_version": 1,
        "submission_version": 1,
        "user_id_list": set()
    }
    assert utils.make_source_id("Small! A dataset data with THE data!!", "Very",
                                test=False) == correct3
    assert utils.make_source_id("very_small_v1.1", "V Ery", test=False) == correct3
    assert utils.make_source_id("very_small_v1", "$V $E RY", test=False) == correct3

    # Double usage should not mutate
    assert utils.make_source_id(correct1["source_id"], "SMITH", test=False) == correct1
    assert utils.make_source_id(correct1["source_name"], "  Smith", test=False) == correct1
    assert utils.make_source_id(correct2["source_id"], "Fox Hound", test=True) == correct2
    assert utils.make_source_id(correct2["source_name"], "FOXHound", test=True) == correct2
    assert utils.make_source_id(correct3["source_id"], "Very", test=False) == correct3
    assert utils.make_source_id(correct3["source_name"], "V. Ery", test=False) == correct3

    # TODO: Set/find known static source_id in StatusDB
    # With previous versions
    # res = utils.make_source_id("", test=False)
    # assert res["version"] > 1
    # assert res["source_name"] == ""
    # assert res["source_id"].endswith(str(res["version"]))


def test_split_source_id():
    # Standard form
    assert utils.split_source_id("_test_foo_bar_study_v1.1") == {
        "success": True,
        "source_name": "_test_foo_bar_study",
        "source_id": "_test_foo_bar_study_v1.1",
        "search_version": 1,
        "submission_version": 1
    }
    assert utils.split_source_id("study_v8_engines_v2.8") == {
        "success": True,
        "source_name": "study_v8_engines",
        "source_id": "study_v8_engines_v2.8",
        "search_version": 2,
        "submission_version": 8
    }
    # Incorrect form
    assert utils.split_source_id("just_this") == {
        "success": False,
        "source_name": "just_this",
        "source_id": "just_this",
        "search_version": 0,
        "submission_version": 0
    }
    # Invalid forms
    # NOTE: Should never happen, but should be handled appropriately anyway
    assert utils.split_source_id("study_v3.4_engines_v2.8") == {
        "success": True,
        "source_name": "study_v3.4_engines",
        "source_id": "study_v3.4_engines_v2.8",
        "search_version": 2,
        "submission_version": 8
    }
    assert utils.split_source_id("just_v3.4_this") == {
        "success": False,
        "source_name": "just_v3.4_this",
        "source_id": "just_v3.4_this",
        "search_version": 0,
        "submission_version": 0
    }

    # TODO: Remove legacy-form support
    # Legacy-dash form
    assert utils.split_source_id("_test_foo_bar_study_v1-1") == {
        "success": True,
        "source_name": "_test_foo_bar_study",
        "source_id": "_test_foo_bar_study_v1.1",
        "search_version": 1,
        "submission_version": 1
    }
    assert utils.split_source_id("study_v8_engines_v2-8") == {
        "success": True,
        "source_name": "study_v8_engines",
        "source_id": "study_v8_engines_v2.8",
        "search_version": 2,
        "submission_version": 8
    }
    # Legacy-merge form
    assert utils.split_source_id("_test_old_oqmd_v13") == {
        "success": True,
        "source_name": "_test_old_oqmd",
        "source_id": "_test_old_oqmd_v13.13",
        "search_version": 13,
        "submission_version": 13
    }
    assert utils.split_source_id("ser_v1_ng_stuff_v2") == {
        "success": True,
        "source_name": "ser_v1_ng_stuff",
        "source_id": "ser_v1_ng_stuff_v2.2",
        "search_version": 2,
        "submission_version": 2
    }


def test_scan_table():
    # Regular usage
    # Requires multiple statuses in DB
    # TODO: Set/find known static source_id in StatusDB
    res = utils.scan_table(table_name="status", )
    assert res["success"]
    count1 = len(res["results"])
    sample1 = res["results"][0]

    res = utils.scan_table(table_name="status", fields="source_id")
    assert res["success"]
    # Fields arg should not restrict results
    assert len(res["results"]) == count1
    # Only 'source_id' should be in results
    assert all([("source_id" in entry.keys() and len(entry.keys()) == 1)
                for entry in res["results"]])

    res = utils.scan_table(table_name="status", fields=["source_id", "test"])
    assert res["success"]
    assert len(res["results"]) == count1
    assert all([("source_id" in entry.keys() and "test" in entry.keys() and len(entry.keys()) == 2)
                for entry in res["results"]])

    res = utils.scan_table(table_name="status", filters=("code", "!=", None))  # Exists
    assert res["success"]
    assert len(res["results"]) == count1

    res = utils.scan_table(table_name="status", filters=[("source_id", "!=", sample1["source_id"])])
    assert res["success"]
    assert len(res["results"]) == count1 - 1

    res = utils.scan_table(table_name="status", filters=[("source_id", "==", sample1["source_id"])])
    assert res["success"]
    assert len(res["results"]) == 1
    assert res["results"][0] == sample1

    res = utils.scan_table(table_name="status",
                           filters=("submission_time", ">", sample1["submission_time"]))
    assert res["success"]
    count2 = len(res["results"])
    assert count2 < count1

    res = utils.scan_table(table_name="status",
                           filters=[("submission_time", ">", sample1["submission_time"]),
                                    ("code", "<", sample1["code"])])
    assert res["success"]

    # Errors
    res = utils.scan_table(table_name="status", fields=True)
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_table(table_name="status", filters=("field", "[]", "ab"))
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_table(table_name="status", filters=("field", "in", "ab"))
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_table(table_name="status", filters=("field", "@", "ab"))
    assert not res["success"] and res.get("error", None) is not None
    res = utils.scan_table(table_name="status", filters={"field": "val"})
