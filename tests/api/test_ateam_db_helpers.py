from agr_literature_service.api.crud import ateam_db_helpers


def test_atp_to_name_subset_with_monkeypatched_globals(monkeypatch):
    # 1) Prepare a fake mapping and prevent the real loader from running.
    fake_mapping = {
        "ATP:0000274": "Manual indexing needed",
        "ATP:0000275": "Manual indexing in progress",
    }
    monkeypatch.setattr(ateam_db_helpers, "atp_to_name", fake_mapping)
    monkeypatch.setattr(
        ateam_db_helpers,
        "load_name_to_atp_and_relationships",
        lambda: (_ for _ in ()).throw(AssertionError("Should not hit real loader"))
    )

    # 2) Call the subset function with only keys we know about
    curies = ["ATP:0000274", "ATP:0000275"]
    result = ateam_db_helpers.atp_to_name_subset(curies)

    # 3) Assert we got exactly our fake mapping back
    assert result == fake_mapping
