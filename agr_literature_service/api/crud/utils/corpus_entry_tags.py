"""The workflow tags a reference receives on entering a MOD's corpus, per MOD.

One table for both paths that grant them, so they cannot drift:

* the API path -- ``mod_corpus_association_crud`` (``create``, ``patch``,
  ``batch_update_corpus``);
* the ingest path -- ``lit_processing.data_ingest.utils.db_write_utils``
  (``update_mod_corpus_associations`` from the nightly DQM run, and
  ``post_reference_to_db``).

Each entry is ``(ontology name, curie, every state of that tag's own workflow)``.
A tag is only granted when the reference is in none of those states for the MOD:
a reference already in its workflow (a curator moved it on, or WB imported its
current state) must not get a second state of the same workflow, which would
make ``_get_current_workflow_tag_db_obj``'s ``.one_or_none()`` raise
``MultipleResultsFound`` (a 500) and put the paper back in the queue.

ZFIN: see ``zfin_corpus_entry`` (SCRUM-5764).

WB (SCRUM-6487) is deliberately not in ``CORPUS_ENTRY_TAGS``: "author-person
curation needed" is opt-in, granted only when a curator sorts a paper inside
the corpus with the sort page's "Author-Person curation" checkbox checked
(``mod_corpus_association_crud.patch`` with ``author_person_curation_needed``).
Ingest, batch moves and other corpus edits never grant it. There is no
backfill: WB curators are importing the current state of the papers already in
the corpus themselves.
"""

from typing import Dict, List, Tuple

from agr_literature_service.api.crud.utils.zfin_corpus_entry import ZFIN_CORPUS_ENTRY_TAGS

AUTHOR_PERSON_CURATION_NEEDED = "ATP:0000109"

# MOD -> the author-person curation tag granted on request (SCRUM-6487).
AUTHOR_PERSON_CURATION_TAGS: Dict[str, List[Tuple[str, str, List[str]]]] = {
    "WB": [
        (
            "author-person curation needed",
            AUTHOR_PERSON_CURATION_NEEDED,
            ["ATP:0000109",   # author-person curation needed
             "ATP:0000377",   # author-person curation in progress
             "ATP:0000376",   # author-person curation blocked
             "ATP:0000378"],  # author-person curation complete
        ),
    ],
}

MANUAL_INDEXING_NEEDED = "ATP:0000274"
EMAIL_EXTRACTION_NEEDED = "ATP:0000358"

# SGD (SCRUM-6845): every SGD paper starts in "manual indexing needed"; for the other MODs
# the first manual indexing state is a curator's or classifier's transition from the
# manual indexing process term.
MANUAL_INDEXING_ENTRY_TAG: Tuple[str, str, List[str]] = (
    "manual indexing needed",
    MANUAL_INDEXING_NEEDED,
    # every child of manual indexing (ATP:0000273)
    ["ATP:0000274",   # manual indexing needed
     "ATP:0000275",
     "ATP:0000276",
     "ATP:0000336",
     "ATP:0000343",   # won't manually index
     "ATP:0000359"],  # manual indexing status TBD
)

# SCRUM-6845: email extraction is a job on "email extraction needed" (email_extraction_job),
# which corpus entry starts for the MODs that extract emails; the weekly PubMed/PMC script
# and the full-text extraction work through that job list.
EMAIL_EXTRACTION_ENTRY_TAG: Tuple[str, str, List[str]] = (
    "email extraction needed",
    EMAIL_EXTRACTION_NEEDED,
    ["ATP:0000355",   # email extraction complete
     "ATP:0000356",   # email extraction failed
     "ATP:0000357",   # email extraction in progress
     "ATP:0000358"],  # email extraction needed
)
EMAIL_EXTRACTION_MODS = ("FB", "SGD", "WB", "XB", "ZFIN")

CORPUS_ENTRY_TAGS: Dict[str, List[Tuple[str, str, List[str]]]] = {
    "FB": [EMAIL_EXTRACTION_ENTRY_TAG],
    "SGD": [MANUAL_INDEXING_ENTRY_TAG, EMAIL_EXTRACTION_ENTRY_TAG],
    "WB": [EMAIL_EXTRACTION_ENTRY_TAG],
    "XB": [EMAIL_EXTRACTION_ENTRY_TAG],
    "ZFIN": ZFIN_CORPUS_ENTRY_TAGS + [EMAIL_EXTRACTION_ENTRY_TAG],
}
