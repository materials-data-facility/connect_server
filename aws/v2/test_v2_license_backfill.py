import json

from v2.scripts.backfill_license_mdf_open import CC_BY_4, plan_row


def _rec(org, license=None, as_string=True, top_org=True):
    mdata = {"title": "t", "organization": org}
    if license is not None:
        mdata["license"] = license
    rec = {"source_id": "x", "version": "1.0", "dataset_mdata": json.dumps(mdata) if as_string else mdata}
    if top_org:
        rec["organization"] = org
    return rec


def test_mdf_open_without_license_gets_cc_by_4():
    new, reason = plan_row(_rec("MDF Open"))
    assert reason == "set-cc-by-4.0"
    assert json.loads(new["dataset_mdata"])["license"] == CC_BY_4
    assert new["updated_at"] is None if "updated_at" in new else True  # never bumped


def test_empty_license_variants_are_treated_as_missing():
    for empty in ("", {}, {"name": ""}, []):
        new, reason = plan_row(_rec("MDF Open", license=empty))
        assert reason == "set-cc-by-4.0", empty


def test_existing_license_is_never_changed():
    for lic in ({"name": "Creative Commons Attribution 4.0"}, {"name": "MIT", "identifier": "MIT"}, "CC0"):
        new, reason = plan_row(_rec("MDF Open", license=lic))
        assert new is None and reason == "has-license", lic


def test_other_collections_are_never_touched():
    for org in ("Foundry", "CHiMaD", "", None, "mdf open"):
        new, reason = plan_row(_rec(org))
        assert new is None and reason == "other-collection", org


def test_map_encoded_metadata_keeps_map_encoding():
    new, _ = plan_row(_rec("MDF Open", as_string=False))
    assert isinstance(new["dataset_mdata"], dict)
    assert new["dataset_mdata"]["license"] == CC_BY_4


def test_v1_migration_defaults_mdf_open_license_only_when_missing():
    from v2.metadata import migrate_v1_payload

    v1 = {"dc": {"titles": [{"title": "T"}], "creators": [{"creatorName": "A, B"}]}, "mdf": {"organization": "MDF Open"}}
    out = migrate_v1_payload(v1)
    assert out["license"]["identifier"] == "CC-BY-4.0"

    v1_explicit = {"dc": {"titles": [{"title": "T"}], "creators": [{"creatorName": "A, B"}],
                          "rightsList": [{"rights": "MIT"}]}, "mdf": {"organization": "MDF Open"}}
    assert migrate_v1_payload(v1_explicit)["license"]["name"] == "MIT"

    other = {"dc": {"titles": [{"title": "T"}], "creators": [{"creatorName": "A, B"}]}, "mdf": {"organization": "Foundry"}}
    assert not migrate_v1_payload(other).get("license")
