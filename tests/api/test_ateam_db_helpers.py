from agr_literature_service.api.crud import ateam_db_helpers


def test_atp_to_name_subset_with_monkeypatched_globals(monkeypatch):
    # 1) Prepare a fake atp_to_name dict, and prevent the real loader from running.
    fake_mapping = {
        "ATP:0000274": "Manual indexing needed",
        "ATP:0000275": "Manual indexing in progress",
        # note: we deliberately leave out ATP:0000276
    }
    monkeypatch.setattr(ateam_db_helpers, "atp_to_name", fake_mapping)
    monkeypatch.setattr(
        ateam_db_helpers,
        "load_name_to_atp_and_relationships",
        lambda: (_ for _ in ()).throw(AssertionError("Should not hit real loader"))
    )

    # 2) Call the subset function
    curies = ["ATP:0000274", "ATP:0000275", "ATP:0000276"]
    result = ateam_db_helpers.atp_to_name_subset(curies)

    # 3) Assert we got only the keys present in our fake mapping
    assert result == {
        "ATP:0000274": "Manual indexing needed",
        "ATP:0000275": "Manual indexing in progress",
    }
