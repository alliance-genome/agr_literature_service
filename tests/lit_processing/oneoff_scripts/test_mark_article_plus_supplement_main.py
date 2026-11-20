"""The SCRUM-6753 backfill picks references with a PMC supplement PDF and no
PMC main file; a MOD's own main PDF does not exclude a reference."""
import importlib.util
from pathlib import Path
from typing import Any, cast

from agr_literature_service.api.models import CrossReferenceModel, ModModel, ReferenceModel, \
    ReferencefileModAssociationModel, ReferencefileModel
from ...fixtures import db  # noqa

# ticket-prefixed file name, so load it by path (see test_reclassify_htp_supplements)
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6753_mark_article_plus_supplement_main.py"
)
_spec = importlib.util.spec_from_file_location("mark_article_plus_supplement_main", _SCRIPT)
assert _spec is not None and _spec.loader is not None
mark_article_plus_supplement_main = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(mark_article_plus_supplement_main)

get_candidates = cast(Any, mark_article_plus_supplement_main).get_candidates
get_article_xml = cast(Any, mark_article_plus_supplement_main).get_article_xml


def _reference(db, n):  # noqa
    ref = ReferenceModel(curie=f"AGRKB:APS-TEST-{n}", category="research_article")
    db.add(ref)
    db.flush()
    db.add(CrossReferenceModel(reference_id=ref.reference_id, curie=f"PMCID:PMC99{n}",
                               curie_prefix="PMCID"))
    return ref.reference_id


def _file(db, reference_id, name, file_class, mod_id=None, extension="pdf"):  # noqa
    row = ReferencefileModel(reference_id=reference_id, display_name=name, file_class=file_class,
                             file_publication_status="final", file_extension=extension,
                             pdf_type="pdf" if extension == "pdf" else None,
                             md5sum=f"md5-{reference_id}-{name}.{extension}")
    db.add(row)
    db.flush()
    db.add(ReferencefileModAssociationModel(referencefile_id=row.referencefile_id, mod_id=mod_id))
    return row.referencefile_id


def test_candidates(db):  # noqa
    fb = db.query(ModModel).filter_by(abbreviation="FB").one_or_none()
    if fb is None:
        fb = ModModel(abbreviation="FB", short_name="FB", full_name="FlyBase")
        db.add(fb)
        db.flush()
    # PMC supplements only: a candidate with both its PMC PDFs
    no_main = _reference(db, 1)
    _file(db, no_main, "mmc1", "supplement")
    _file(db, no_main, "mmc2", "supplement")
    # a MOD-uploaded main PDF only (like FB's): still a candidate
    mod_main = _reference(db, 2)
    _file(db, mod_main, "mmc5", "supplement")
    _file(db, mod_main, "25484300", "main", mod_id=fb.mod_id)
    # already has a PMC main PDF: not a candidate
    pmc_main = _reference(db, 3)
    _file(db, pmc_main, "mmc1", "supplement")
    _file(db, pmc_main, "article", "main")
    # a supplement uploaded by a MOD, not from PMC: not a candidate
    mod_suppl = _reference(db, 4)
    _file(db, mod_suppl, "s1", "supplement", mod_id=fb.mod_id)
    db.commit()

    candidates = get_candidates(db)

    assert candidates[no_main][0] == "PMC991"
    assert sorted(name for _, name in candidates[no_main][1]) == ["mmc1", "mmc2"]
    assert [name for _, name in candidates[mod_main][1]] == ["mmc5"]
    assert pmc_main not in candidates
    assert mod_suppl not in candidates


def test_article_xml(db):  # noqa
    fb = db.query(ModModel).filter_by(abbreviation="FB").one_or_none()
    if fb is None:
        fb = ModModel(abbreviation="FB", short_name="FB", full_name="FlyBase")
        db.add(fb)
        db.flush()
    reference_id = _reference(db, 5)
    article = _file(db, reference_id, "PMC995.1", "supplement", extension="xml")
    _file(db, reference_id, "mmc3", "supplement", extension="xml")      # a supplementary XML
    _file(db, reference_id, "PMC995", "supplement", mod_id=fb.mod_id, extension="xml")  # not from PMC
    db.commit()

    assert [row.referencefile_id for row in get_article_xml(db, reference_id, "PMC995")] == [article]
