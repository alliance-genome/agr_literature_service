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

WB (SCRUM-6487): "author-person curation needed" on corpus entry, so the paper
gets author-person curation before community curation is ready. It is granted
at corpus entry, without waiting on a PDF. There is deliberately no backfill:
WB curators are importing the current state of the papers already in the
corpus themselves.
"""

from typing import Dict, List, Tuple

from agr_literature_service.api.crud.utils.zfin_corpus_entry import ZFIN_CORPUS_ENTRY_TAGS

AUTHOR_PERSON_CURATION_NEEDED = "ATP:0000109"

WB_CORPUS_ENTRY_TAGS: List[Tuple[str, str, List[str]]] = [
    (
        "author-person curation needed",
        AUTHOR_PERSON_CURATION_NEEDED,
        ["ATP:0000109",   # author-person curation needed
         "ATP:0000377",   # author-person curation in progress
         "ATP:0000376",   # author-person curation blocked
         "ATP:0000378"],  # author-person curation complete
    ),
]

CORPUS_ENTRY_TAGS: Dict[str, List[Tuple[str, str, List[str]]]] = {
    "ZFIN": ZFIN_CORPUS_ENTRY_TAGS,
    "WB": WB_CORPUS_ENTRY_TAGS,
}
