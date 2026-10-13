"""Archive uploads (SCRUM-6633): a MOD's existing main PDFs are attached to
their references WITHOUT queuing text conversion, through
POST /reference/referencefile/pdf_archive_upload/ (curators only)."""
import io
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, UploadFile
from starlette.testclient import TestClient

from agr_literature_service.api import database
from agr_literature_service.api.auth import DEFAULT_BYPASS_USER, get_authenticated_user
from agr_literature_service.api.crud import referencefile_archive_crud as archive
from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ModCorpusAssociationModel,
    ModModel,
    ReferenceModel,
    ReferencefileModel,
    WorkflowTagModel,
)
from agr_literature_service.api.routers import referencefile_router
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db, load_name_to_atp_and_relationships_mock  # noqa

FILES_UPLOADED = "ATP:0000134"
TEXT_CONVERSION_NEEDED = "ATP:0000162"
CRUD = "agr_literature_service.api.crud.referencefile_archive_crud"


@pytest.mark.parametrize("mod, filename, expected", [
    ("FB", "FBrf0123456.pdf", "FB:FBrf0123456"),
    ("FB", "FBrf0123456.PDF", "FB:FBrf0123456"),
    ("FB", "/some/dir/FBrf0123456.pdf", "FB:FBrf0123456"),
    ("WB", "WBPaper00031514.pdf", "WB:WBPaper00031514"),
    ("XB", "XB-ART-59369.pdf", "Xenbase:XB-ART-59369"),
    ("ZFIN", "ZDB-PUB-200229-13.pdf", "ZFIN:ZDB-PUB-200229-13"),
    ("MGI", "5427319.pdf", "MGI:5427319"),
    ("SGD", "S000126523.pdf", "SGD:S000126523"),
    ("RGD", "1311419.pdf", "RGD:1311419"),
    ("FB", "fbrf0123456.pdf", None),
    ("FB", "FBrf012345.pdf", None),
    ("FB", "FBrf0123456.txt", None),
    ("FB", "FBrf0123456_temp.pdf", None),
    ("FB", ".pdf", None),
    ("WB", "FBrf0123456.pdf", None),
    ("NOPE", "FBrf0123456.pdf", None),
])
def test_archive_xref_curie(mod, filename, expected):
    assert archive.archive_xref_curie(mod, filename) == expected


def _upload_file(name="FBrf0000001.pdf", content=b"%PDF-1.4 archive"):
    return UploadFile(file=io.BytesIO(content), filename=name)


class TestSetFileUploadedStatus:
    """The status is set without transition actions: create() or a plain
    in-place update, never transition_to_workflow_status."""

    def _run(self, current):
        mock_db = MagicMock()
        with patch(f"{CRUD}._get_current_workflow_tag_db_obj", return_value=current) as lookup, \
                patch(f"{CRUD}.create_wft") as create:
            change = archive.set_file_uploaded_status(mock_db, "AGRKB:101000000000001", "WB")
        lookup.assert_called_once_with(mock_db, "AGRKB:101000000000001", "ATP:0000140", "WB")
        return change, create, mock_db

    def test_creates_tag_when_paper_has_none(self):
        change, create, _ = self._run(None)
        assert change == "created"
        data = create.call_args.args[1]
        assert data.workflow_tag_id == FILES_UPLOADED
        assert data.mod_abbreviation == "WB"
        assert data.reference_curie == "AGRKB:101000000000001"

    def test_overwrites_other_tag_in_place(self):
        current = MagicMock(workflow_tag_id="ATP:0000141")  # file needed
        change, create, mock_db = self._run(current)
        assert change == "ATP:0000141 -> ATP:0000134"
        assert current.workflow_tag_id == FILES_UPLOADED
        mock_db.commit.assert_called_once()
        create.assert_not_called()

    def test_leaves_files_uploaded_alone(self):
        current = MagicMock(workflow_tag_id=FILES_UPLOADED)
        change, create, mock_db = self._run(current)
        assert change is None
        mock_db.commit.assert_not_called()
        create.assert_not_called()

    def test_module_never_imports_the_action_running_transitions(self):
        assert not hasattr(archive, "transition_to_workflow_status")
        assert not hasattr(archive, "transition_WFT_for_uploaded_file")


def _ref(reference_id):
    return MagicMock(reference_id=reference_id, curie=f"AGRKB:{reference_id}")


def _archive_upload(file, reference=None, in_corpus=True, existing=(), same_file_not_main=False, **kwargs):
    """archive_upload with every database collaborator stubbed."""
    mock_db = MagicMock()
    with patch(f"{CRUD}.find_reference", return_value=reference) as find, \
            patch(f"{CRUD}.check_if_paper_in_corpus", return_value=in_corpus), \
            patch(f"{CRUD}.mod_main_pdf_md5s", return_value=set(existing)), \
            patch(f"{CRUD}.has_same_file_not_main", return_value=same_file_not_main), \
            patch(f"{CRUD}.file_upload") as upload, \
            patch(f"{CRUD}.set_file_uploaded_status", return_value="created") as status:
        result = archive.archive_upload(mock_db, "FB", file, **kwargs)
    return result, find, upload, status


class TestArchiveUpload:

    def test_uploads_without_post_upload_actions_and_sets_status(self):
        result, find, upload, status = _archive_upload(_upload_file(), reference=_ref(1))
        assert result == {"file_name": "FBrf0000001.pdf", "status": "uploaded", "xref_curie": "FB:FBrf0000001",
                          "reference_curie": "AGRKB:1", "workflow_change": "created"}
        find.assert_called_once()
        assert find.call_args.args[1] == "FB:FBrf0000001"
        kwargs = upload.call_args.kwargs
        assert kwargs["skip_post_upload_actions"] is True
        assert kwargs["upload_if_already_converted"] is True
        assert kwargs["metadata"] == {
            "reference_curie": "AGRKB:1", "display_name": "FBrf0000001", "file_class": "main",
            "file_publication_status": "final", "file_extension": "pdf", "pdf_type": "pdf",
            "is_annotation": False, "mod_abbreviation": "FB"}
        assert kwargs["file"].filename == "FBrf0000001.pdf"
        status.assert_called_once()
        assert status.call_args.args[1:] == ("AGRKB:1", "FB")

    def test_bad_name(self):
        result, find, upload, status = _archive_upload(_upload_file("notes.pdf"))
        assert result["status"] == "bad_name" and result["xref_curie"] is None
        find.assert_not_called()
        upload.assert_not_called()

    def test_no_xref(self):
        result, _, upload, status = _archive_upload(_upload_file(), reference=None)
        assert result["status"] == "no_xref" and result["xref_curie"] == "FB:FBrf0000001"
        upload.assert_not_called()
        status.assert_not_called()

    def test_same_file_not_main(self):
        result, _, upload, status = _archive_upload(_upload_file(), reference=_ref(6), same_file_not_main=True)
        assert result["status"] == "same_file_not_main"
        upload.assert_not_called()
        status.assert_not_called()

    def test_not_in_corpus(self):
        result, _, upload, status = _archive_upload(_upload_file(), reference=_ref(2), in_corpus=False)
        assert result["status"] == "not_in_corpus" and result["reference_curie"] == "AGRKB:2"
        upload.assert_not_called()
        status.assert_not_called()

    def test_already_loaded_still_sets_status(self):
        file = _upload_file()
        md5 = archive.file_md5(file)
        result, _, upload, status = _archive_upload(file, reference=_ref(3), existing={md5})
        assert result["status"] == "already_loaded" and result["workflow_change"] == "created"
        upload.assert_not_called()
        status.assert_called_once()

    def test_has_other_mod_pdf(self):
        result, _, upload, status = _archive_upload(_upload_file(), reference=_ref(4), existing={"other-md5"})
        assert result["status"] == "has_other_mod_pdf"
        upload.assert_not_called()
        status.assert_not_called()

    def test_include_existing_uploads_alongside(self):
        result, _, upload, _ = _archive_upload(_upload_file(), reference=_ref(4), existing={"other-md5"},
                                               include_existing=True)
        assert result["status"] == "uploaded"
        upload.assert_called_once()

    @pytest.mark.parametrize("existing", [(), ("other-md5",)])
    def test_dry_run_writes_nothing(self, existing):
        result, _, upload, status = _archive_upload(_upload_file(), reference=_ref(5), existing=existing,
                                                    include_existing=True, dry_run=True)
        assert result["status"] == "would_upload"
        upload.assert_not_called()
        status.assert_not_called()

    def test_dry_run_already_loaded_leaves_status(self):
        file = _upload_file()
        result, _, _, status = _archive_upload(file, reference=_ref(3), existing={archive.file_md5(file)},
                                               dry_run=True)
        assert result["status"] == "already_loaded" and result["workflow_change"] is None
        status.assert_not_called()

    def test_unknown_mod_is_422(self):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.one_or_none.return_value = None
        with pytest.raises(HTTPException) as exc:
            archive.archive_upload(mock_db, "NOPE", _upload_file())
        assert exc.value.status_code == 422


def _id_token_user(*groups):
    return {"token_type": "id", "sub": "a-uuid", "email": "curator@example.org", "cognito:groups": list(groups)}


class TestRequireCognitoCurator:

    @pytest.mark.parametrize("user", [None, DEFAULT_BYPASS_USER])
    def test_no_cognito_login_is_401(self, user):
        with pytest.raises(HTTPException) as exc:
            referencefile_router.require_cognito_curator(user, "FB")
        assert exc.value.status_code == 401

    @pytest.mark.parametrize("groups", [(), ("WormBaseCurator",)])
    def test_without_access_to_the_mod_is_403(self, groups):
        with pytest.raises(HTTPException) as exc:
            referencefile_router.require_cognito_curator(_id_token_user(*groups), "FB")
        assert exc.value.status_code == 403

    @pytest.mark.parametrize("user", [
        _id_token_user("FlyBaseCurator"),
        _id_token_user("AllianceDeveloper"),
        {"token_type": "access", "sub": "service-client", "client_id": "service-client"},
    ])
    def test_curator_of_the_mod_admin_or_service_account_is_allowed(self, user):
        referencefile_router.require_cognito_curator(user, "FB")


class TestEndpoint:
    """Wiring only: auth, query parameters and the response model."""

    @pytest.fixture
    def client(self):
        app.dependency_overrides[database.get_db] = lambda: MagicMock()
        yield TestClient(app)
        app.dependency_overrides.clear()

    def _post(self, client, user, params="mod_abbreviation=FB", result=None):
        app.dependency_overrides[get_authenticated_user] = lambda: user
        result = result or {"file_name": "FBrf0000001.pdf", "status": "uploaded", "xref_curie": "FB:FBrf0000001",
                            "reference_curie": "AGRKB:1", "workflow_change": "created"}
        with patch.object(referencefile_router, "set_global_user_from_cognito"), \
                patch.object(referencefile_router.referencefile_archive_crud, "archive_upload",
                             return_value=result) as crud:
            response = client.post(f"/reference/referencefile/pdf_archive_upload/?{params}",
                                   files={"file": ("FBrf0000001.pdf", b"%PDF", "application/pdf")})
        return response, crud

    def test_curator_upload(self, client):
        response, crud = self._post(client, _id_token_user("FlyBaseCurator"),
                                    params="mod_abbreviation=FB&dry_run=true&include_existing=true")
        assert response.status_code == 200
        assert response.json()["status"] == "uploaded"
        assert crud.call_args.args[1] == "FB"
        assert crud.call_args.kwargs == {"include_existing": True, "dry_run": True}

    def test_bypass_user_is_401(self, client):
        response, crud = self._post(client, DEFAULT_BYPASS_USER)
        assert response.status_code == 401
        crud.assert_not_called()

    def test_other_mod_curator_is_403(self, client):
        response, crud = self._post(client, _id_token_user("WormBaseCurator"))
        assert response.status_code == 403
        crud.assert_not_called()


@pytest.fixture
def fb_paper(db):  # noqa
    """A reference in the FB corpus carrying FB:FBrf0000001."""
    populate_test_mods()
    reference = ReferenceModel(curie="AGRKB:101000500001", category="research_article")
    db.add(reference)
    db.commit()
    db.refresh(reference)
    fb = db.query(ModModel).filter(ModModel.abbreviation == "FB").one()
    db.add(CrossReferenceModel(curie="FB:FBrf0000001", curie_prefix="FB", reference_id=reference.reference_id))
    db.add(ModCorpusAssociationModel(reference_id=reference.reference_id, mod_id=fb.mod_id, corpus=True,
                                     mod_corpus_sort_source=ModCorpusSortSourceType.Mod_pubmed_search))
    db.commit()
    return reference


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
def test_archive_upload_against_the_database(db, fb_paper):  # noqa
    """End to end on a real database: the file is stored as an FB main PDF, the
    FB file-upload status is "files uploaded", and nothing reaches text
    conversion. S3 is stubbed; the post-upload actions must never run."""
    never = AssertionError("must not run for an archive upload")
    with patch("agr_literature_service.api.crud.referencefile_crud.boto3"), \
            patch("agr_literature_service.api.crud.referencefile_crud.upload_file_to_bucket") as s3_upload, \
            patch("agr_literature_service.api.crud.referencefile_crud.cleanup_old_pdf_file", side_effect=never), \
            patch("agr_literature_service.api.crud.referencefile_crud.transition_WFT_for_uploaded_file",
                  side_effect=never):
        result = archive.archive_upload(db, "FB", _upload_file())
        again = archive.archive_upload(db, "FB", _upload_file())

    assert result["status"] == "uploaded" and result["reference_curie"] == fb_paper.curie
    assert again["status"] == "already_loaded" and again["workflow_change"] is None
    s3_upload.assert_called_once()

    files = db.query(ReferencefileModel).filter(ReferencefileModel.reference_id == fb_paper.reference_id).all()
    assert [(f.display_name, f.file_class, f.file_publication_status, f.pdf_type) for f in files] == \
        [("FBrf0000001", "main", "final", "pdf")]
    assert [m.mod.abbreviation for m in files[0].referencefile_mods] == ["FB"]

    tags = {t.workflow_tag_id for t in
            db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_id == fb_paper.reference_id).all()}
    assert FILES_UPLOADED in tags
    assert TEXT_CONVERSION_NEEDED not in tags


def test_same_file_as_a_supplement_against_the_database(db, fb_paper):  # noqa
    """The same bytes already on the paper as a supplement: file_upload would
    only link that row to FB, so nothing is uploaded and the status says why."""
    file = _upload_file()
    md5sum = archive.file_md5(file)
    assert archive.find_reference(db, "FB:FBrf0000001").reference_id == fb_paper.reference_id
    assert archive.has_same_file_not_main(db, fb_paper.reference_id, md5sum) is False

    db.add(ReferencefileModel(reference_id=fb_paper.reference_id, display_name="supplement",
                              file_class="supplement", file_publication_status="final",
                              file_extension="pdf", pdf_type=None, md5sum=md5sum, is_annotation=False))
    db.commit()

    assert archive.has_same_file_not_main(db, fb_paper.reference_id, md5sum) is True
    with patch("agr_literature_service.api.crud.referencefile_crud.upload_file_to_bucket") as s3_upload:
        assert archive.archive_upload(db, "FB", file)["status"] == "same_file_not_main"
    s3_upload.assert_not_called()
