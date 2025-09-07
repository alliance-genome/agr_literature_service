import pytest
from starlette.testclient import TestClient
from fastapi import status
from sqlalchemy import text
import numpy as np
if not hasattr(np, "float_"):
    np.float_ = np.float64

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    ReferenceModel,
    ReferencefileModel,
)
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from agr_literature_service.api.crud.referencefile_crud import (
    create_metadata,
    get_main_pdf_referencefile_id,
    get_main_pdf_referencefile_ids_for_ref_curies_list,
    set_referencefile_mods,
    check_file_upload_status_change,
    transition_WFT_for_uploaded_file,
    cleanup_wft_tet_tags_for_deleted_main_pdf,
    find_first_available_display_name,
)
from agr_literature_service.api.crud.referencefile_mod_utils import create as create_ref_file_mod
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.crud.referencefile_crud import file_paths_in_dir

from .test_reference import test_reference # noqa
from .test_reference import test_reference as test_reference2 # noqa
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa


@pytest.fixture
def mk_file(db): # noqa
    """Factory: create a Referencefile row (metadata only, no S3)."""
    def _mk_file(
        ref_curie: str,
        *,
        display: str = "Bob",
        ext: str = "pdf",
        pub: str = "final",
        fclass: str = "main",
        pdf_type: str = "pdf",
        md5_suffix: str | None = None,
        mod_abbreviation=None,
    ) -> int:
        md5_suffix = md5_suffix or display
        payload = ReferencefileSchemaPost(
            display_name=display,
            reference_curie=ref_curie,
            file_class=fclass,
            file_publication_status=pub,
            file_extension=ext,
            pdf_type=pdf_type,
            md5sum=f"{display}-{md5_suffix}-md5",
            mod_abbreviation=mod_abbreviation,
        )
        return create_metadata(db, payload)
    return _mk_file


@pytest.fixture
def test_referencefile(db, auth_headers, test_reference): # noqa
    print("***** Adding a test referencefile *****")
    new_referencefile = {
        "display_name": "Bob",
        "reference_curie": test_reference.new_ref_curie,
        "file_class": "main",
        "file_publication_status": "final",
        "file_extension": "pdf",
        "pdf_type": "pdf",
        "md5sum": "1234567890"
    }
    try:
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        referencefile = db.query(ReferencefileModel).filter_by(referencefile_id=new_referencefile_id).one()
        yield referencefile
    finally:
        db.rollback()


class TestReferencefile:

    def test_show_referencefile(self, test_referencefile):
        with TestClient(app) as client:
            response = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}")
            assert response.status_code == status.HTTP_200_OK

    def test_patch_referencefile(self, db, test_referencefile, auth_headers): # noqa
        patch_referencefile = {
            "display_name": "Bob2"
        }
        with TestClient(app) as client:
            response = client.patch(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                    json=patch_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}")
            assert response.json()["display_name"] == patch_referencefile["display_name"]
            ref_file_obj = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == test_referencefile.referencefile_id).one_or_none()
            db.refresh(ref_file_obj)
            assert ref_file_obj.display_name == patch_referencefile["display_name"]

    def test_patch_referencefile_same_name(self, db, test_referencefile, auth_headers, test_reference2):  # noqa
        referencefile_ref2 = {
            "display_name": "Bob",
            "reference_curie": test_reference2.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567891"
        }
        create_metadata(db, ReferencefileSchemaPost(**referencefile_ref2))
        patch_referencefile_ref1 = {
            "reference_curie": test_reference2.new_ref_curie
        }
        with TestClient(app) as client:
            response = client.patch(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                    json=patch_referencefile_ref1, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}")
            assert response.json()["display_name"] == "Bob_1"
            assert response.json()["reference_curie"] == test_reference2.new_ref_curie

            ref_file_obj: ReferencefileModel = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == test_referencefile.referencefile_id).one_or_none()
            db.refresh(ref_file_obj)
            assert ref_file_obj.display_name == "Bob_1"
            assert ref_file_obj.reference.curie == test_reference2.new_ref_curie

    def test_show_all(self, db, test_referencefile): # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}")
            response = client.get(url=f"/reference/referencefile/show_all/"
                                      f"{response_file.json()['reference_curie']}")
            assert len(response.json()) > 0
            assert response.json()[0]["display_name"] == "Bob"

    def test_delete_reference_cascade(self, test_referencefile, auth_headers): # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}")
            client.delete(url=f"/reference/{response_file.json()['reference_curie']}", headers=auth_headers)
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}")
            assert response_file.status_code == status.HTTP_404_NOT_FOUND

    def test_create_referencefile_with_mod(self, db, test_reference, auth_headers): # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": "WB"
        }
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["referencefile_mods"][0]["mod_abbreviation"] == "WB"

    def test_create_referencefile_pmc(self, db, test_reference, auth_headers):  # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": None
        }
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["referencefile_mods"][0]["mod_abbreviation"] is None

    def test_create_referencefile_annotation(self, db, test_reference, auth_headers):  # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": "WB",
            "is_annotation": True
        }
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["is_annotation"] is True

    def test_create_referencefile_pdf_type(self, db, test_reference, auth_headers):  # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "doc",
            "pdf_type": None,
            "md5sum": "1234567890",
            "mod_abbreviation": "WB"
        }
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["pdf_type"] is None
            response = client.get(url=f"/reference/referencefile/show_all/{test_reference.new_ref_curie}")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()[0]["pdf_type"] is None

    def test_merge(self, db, test_referencefile, test_reference, test_reference2, auth_headers):  # noqa                                     
        populate_test_mods()
        referencefile_ref2 = {
            "display_name": "Bob",
            "reference_curie": test_reference2.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": "WB"
        }
        test_referencefile2 = create_metadata(db, ReferencefileSchemaPost(**referencefile_ref2))

        with TestClient(app) as client:
            # Use referencefile_id when referring to test_referencefile and test_referencefile2
            for mod_abbreviation in ["WB", "ZFIN"]:
                new_referencefile_mod = {
                    "referencefile_id": test_referencefile.referencefile_id,  # Access referencefile_id
                    "mod_abbreviation": mod_abbreviation
                }
                client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod, headers=auth_headers)

        for mod_abbreviation in ["FB"]:
            new_referencefile_mod = {
                "referencefile_id": test_referencefile2,  # Use test_referencefile2's referencefile_id
                "mod_abbreviation": mod_abbreviation
            }
            client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod, headers=auth_headers)

            # Pass referencefile_id in the URL instead of the whole object
            request = client.post(url=f"/reference/referencefile/merge/{test_reference.new_ref_curie}/"
                                  f"{test_referencefile.referencefile_id}/{test_referencefile2}", headers=auth_headers)
            assert request.status_code == status.HTTP_201_CREATED

            # Get the updated referencefile and validate the response
            request = client.get(url=f"/reference/referencefile/{test_referencefile2}")
            assert request.json()["display_name"] == "Bob"
            assert request.json()["reference_curie"] == test_reference.new_ref_curie
            assert len(request.json()["referencefile_mods"]) == 4

            mods_to_check = {None, "WB", "FB", "ZFIN"}
            for mod in request.json()["referencefile_mods"]:
                assert mod["mod_abbreviation"] in mods_to_check
                mods_to_check.remove(mod["mod_abbreviation"])
            assert len(mods_to_check) == 0


# The following tests were generated with the help of AI.
class TestMainPdfLookups:
    def test_get_main_pdf_referencefile_id_prefers_mod_specific(self, db, mk_file, test_reference, auth_headers):  # noqa
        populate_test_mods()
        # Make two "main/final/pdf" files for same ref: one PMC(None), one WB
        rf_pmc_id = mk_file(test_reference.new_ref_curie, display="pmc_main")
        rf_wb_id = mk_file(test_reference.new_ref_curie, display="wb_main")

        # Attach mods
        create_ref_file_mod(db, ReferencefileModSchemaPost(referencefile_id=rf_wb_id, mod_abbreviation="WB"))

        # When asking for WB, we should get the WB file
        got = get_main_pdf_referencefile_id(db, test_reference.new_ref_curie, mod_abbreviation="WB")
        assert got == rf_wb_id

        # When no mod specified, prefer the file whose mod is None (PMC)
        got_default = get_main_pdf_referencefile_id(db, test_reference.new_ref_curie)
        assert got_default == rf_pmc_id

    def test_get_main_pdf_referencefile_ids_for_list(self, db, mk_file, test_reference, test_reference2, auth_headers):  # noqa
        populate_test_mods()
        # Ref1: PMC main + WB main
        rf1_wb = mk_file(test_reference.new_ref_curie, display="r1_wb")
        create_ref_file_mod(db, ReferencefileModSchemaPost(referencefile_id=rf1_wb, mod_abbreviation="WB"))

        # Ref2: only PMC main
        rf2_pmc = mk_file(test_reference2.new_ref_curie, display="r2_pmc")

        # Ask for WB view: ref1 should map to WB file; ref2 has no WB, so falls back to PMC
        mapping = get_main_pdf_referencefile_ids_for_ref_curies_list(db, [test_reference.new_ref_curie, test_reference2.new_ref_curie], "WB")
        assert mapping[test_reference.new_ref_curie] == rf1_wb
        assert mapping[test_reference2.new_ref_curie] == rf2_pmc


class TestSetReferencefileMods:
    def test_set_referencefile_mods_shapes_response(self, db, mk_file, test_reference, auth_headers):  # noqa
        populate_test_mods()
        rf_id = mk_file(test_reference.new_ref_curie, display="mods_demo")
        create_ref_file_mod(db, ReferencefileModSchemaPost(referencefile_id=rf_id, mod_abbreviation="WB"))
        rf_obj = db.query(ReferencefileModel).filter_by(referencefile_id=rf_id).one()
        as_dict = {
            "reference_id": rf_obj.reference_id,
            "other": "ok",
        }
        set_referencefile_mods(rf_obj, as_dict)
        # reference_id removed, and we get normalized "referencefile_mods"
        assert "reference_id" not in as_dict
        mods = {m["mod_abbreviation"] for m in as_dict["referencefile_mods"]}
        assert mods == {"WB", None}


class TestCheckFileUploadStatusChange:
    def test_status_change_detects_main_final_pdf_to_other(self, db, mk_file, test_reference):  # noqa
        rf_id = mk_file(test_reference.new_ref_curie, display="will_change")
        rf = db.query(ReferencefileModel).get(rf_id)
        # Change to temp -> should be True
        req = {"file_publication_status": "temp"}
        assert check_file_upload_status_change(db, rf, req) is True

    def test_status_change_no_change_returns_false(self, db, mk_file, test_reference):  # noqa
        rf_id = mk_file(test_reference.new_ref_curie, display="no_change")
        rf = db.query(ReferencefileModel).get(rf_id)
        # No changes
        assert check_file_upload_status_change(db, rf, {}) is False

    def test_status_change_blocked_if_already_converted(self, db, mk_file, monkeypatch, test_reference):  # noqa
        # main/final/pdf with a WB mod so the loop checks WFT
        populate_test_mods()
        rf_id = mk_file(test_reference.new_ref_curie, display="converted")
        create_ref_file_mod(db, ReferencefileModSchemaPost(referencefile_id=rf_id, mod_abbreviation="WB"))
        rf = db.query(ReferencefileModel).get(rf_id)

        # Pretend "already converted to text"
        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.get_current_workflow_status",
            lambda *a, **k: "ATP:0000163"
        )

        with pytest.raises(Exception) as excinfo:
            # Changing away from main/final/pdf without override should raise
            check_file_upload_status_change(db, rf, {"file_publication_status": "temp"})
        assert "File already converted to text" in str(excinfo.value)


class TestTransitionWFT:
    def test_transition_wft_creates_or_transitions(self, db, monkeypatch, test_reference):  # noqa
        # make it a main/final/pdf so it chooses "file_uploaded" tag
        ref_curie = test_reference.new_ref_curie

        calls = {"create": 0, "transition": 0, "cleanup": 0}

        def fake_get_curr(*a, **k):
            # First no tag (create), then some other tag (transition), then already uploaded (noop)
            mod = a[3]
            # Use a counter from closure would be messy; instead switch by mod
            if mod == "WB":
                return None
            if mod == "FB":
                return "ATP:0000999"  # some other tag -> transition
            if mod == "ZFIN":
                return "ATP:0000134"  # already uploaded -> noop
            return None

        def fake_create(*a, **k):
            calls["create"] += 1

        def fake_transition(*a, **k):
            calls["transition"] += 1

        def fake_cleanup(*a, **k):
            calls["cleanup"] += 1

        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.get_current_workflow_status",
            fake_get_curr,
        )
        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.create_wft",
            fake_create,
        )
        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.transition_to_workflow_status",
            fake_transition,
        )
        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.cleanup_wft_tet_tags_for_deleted_main_pdf",
            fake_cleanup,
        )

        # Ensure there are three MODs in corpus for this ref so the function loops over them
        # We can do this by inserting mod_corpus_association via API helper if available; here, use SQL directly.
        # (mods exist because earlier tests call populate_test_mods())
        populate_test_mods()
        # Attach the ref to 3 MODs' corpus = TRUE
        db.execute(text("""
            INSERT INTO mod_corpus_association(reference_id, mod_id, corpus, mod_corpus_sort_source, date_created, date_updated, created_by, updated_by)
            SELECT r.reference_id, m.mod_id, TRUE, 'Manual_creation', NOW(), NOW(), 'default_user', 'default_user'
            FROM reference r, mod m
            WHERE r.curie = :cur AND m.abbreviation IN ('WB','FB','ZFIN')
        """), {"cur": ref_curie})
        db.commit()

        # Run: should 1 create (WB), 1 transition (FB), 0 for ZFIN (already uploaded)
        transition_WFT_for_uploaded_file(
            db, reference_curie=ref_curie, mod_abbreviation=None,
            file_class="main", pdf_type="pdf", file_publication_status="final",
        )
        assert calls["create"] == 1
        assert calls["transition"] == 1
        # change_file_status=False path doesn't call cleanup() in our branches
        assert calls["cleanup"] == 0


class TestCleanupWftTetOnDelete:
    def test_cleanup_wft_tet_tags_respects_access_level(self, db, monkeypatch, test_reference):  # noqa
        # Add a couple of corpus mods so selection logic has something to iterate
        populate_test_mods()
        db.execute(text("""
            INSERT INTO mod_corpus_association(reference_id, mod_id, corpus, mod_corpus_sort_source, date_created, date_updated, created_by, updated_by)
            SELECT r.reference_id, m.mod_id, TRUE, 'Manual_creation', NOW(), NOW(), 'default_user', 'default_user'
            FROM reference r, mod m
            WHERE r.curie = :cur AND m.abbreviation IN ('WB','FB')
        """), {"cur": test_reference.new_ref_curie})
        db.commit()

        ref = db.query(ReferenceModel).filter_by(curie=test_reference.new_ref_curie).one()

        calls = {"reset": [], "delete_non_manual": [], "checked_manual": []}

        def fake_reset(db_, rid, mod_abbr, change_status):
            calls["reset"].append((rid, mod_abbr, change_status))

        def fake_delete_non_manual(db_, rid, mod_abbr):
            calls["delete_non_manual"].append((rid, mod_abbr))

        def fake_has_manual(db_, rid, mod_abbr):
            calls["checked_manual"].append((rid, mod_abbr))
            return False

        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.reset_workflow_tags_after_deleting_main_pdf",
            fake_reset,
        )
        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.delete_non_manual_tets",
            fake_delete_non_manual,
        )
        monkeypatch.setattr(
            "agr_literature_service.api.crud.referencefile_crud.has_manual_tet",
            fake_has_manual,
        )

        # Case 1: limited access (WB) -> only WB processed
        cleanup_wft_tet_tags_for_deleted_main_pdf(
            db, reference_id=ref.reference_id, all_mods=set(), access_level="WB", change_file_status=False
        )
        assert [m for (_, m, _) in calls["reset"]] == ["WB"]
        assert [m for (_, m) in calls["delete_non_manual"]] == ["WB"]
        assert [m for (_, m) in calls["checked_manual"]] == ["WB"]

        # Case 2: all_access with explicit set (no PMC) -> use given all_mods
        calls = {"reset": [], "delete_non_manual": [], "checked_manual": []}
        cleanup_wft_tet_tags_for_deleted_main_pdf(
            db, reference_id=ref.reference_id, all_mods={"WB", "FB"}, access_level="all_access", change_file_status=False
        )
        assert sorted([m for (_, m, _) in calls["reset"]]) == ["FB", "WB"]


class TestFindFirstAvailableDisplayName:
    def test_find_first_available_appends_suffix(self, db, mk_file, test_reference):  # noqa
        # Create a file named "Bob.pdf"
        _ = mk_file(test_reference.new_ref_curie, display="Bob", ext="pdf")
        # Ask for another "Bob.pdf" -> expect "Bob_1"
        name = find_first_available_display_name("Bob", "pdf", test_reference.new_ref_curie, db)
        assert name == "Bob_1"


class TestFilePathsInDir:
    def test_file_paths_in_dir_lists_files(self, tmp_path):
        # Make nested files
        base = tmp_path
        (base / "a").mkdir()
        (base / "a" / "x.txt").write_text("hi")
        (base / "b").mkdir()
        (base / "b" / "y.txt").write_text("yo")

        paths = set(file_paths_in_dir(str(base)))
        assert str(base / "a" / "x.txt") in paths
        assert str(base / "b" / "y.txt") in paths
