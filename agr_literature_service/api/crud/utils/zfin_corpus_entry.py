"""SCRUM-5764: the workflow tags a ZFIN reference receives on entering the corpus.

Shared by the two paths that grant them so the pair cannot drift:

* the API path -- ``mod_corpus_association_crud`` (``create``, ``patch``,
  ``batch_update_corpus``);
* the ingest path -- ``lit_processing.data_ingest.utils.db_write_utils``
  (``add_zfin_corpus_entry_tags``), reached from the nightly DQM run and from
  ``post_reference_to_db``.

Each entry is ``(ontology name, curie, every state of that tag's own workflow)``.
A reference already sitting in any of those states has entered the workflow, so
the "needed" tag must not be granted again: two states of one workflow on the
same (reference, mod) make ``_get_current_workflow_tag_db_obj``'s
``.one_or_none()`` raise ``MultipleResultsFound`` (a 500 on the next status read
for that process), and the paper would also reappear in the queue after a
curator moved it on. ``destroy()`` removes only the file-needed tag, so this is
reachable through destroy-then-recreate as well as through the ingest path.

The molecular probe list is deliberately non-contiguous. ATP:0000381/382/384/386
are the GENERIC "abstract classification" needed/in progress/failed/complete
states under ATP:0000379, and the probe ids below are their children --
confirmed against ATP.owl @ origin/main and against the live closure on
curation-db, which gives ATP:0000380 -> ATP:0000381 -> ATP:0000379 ->
ATP:0000177. There is therefore no probe-specific process node, and
``get_current_workflow_status`` cannot be used to expand this set: ATP:0000379
would also pull in sibling abstract classifiers such as SCRUM-5765 protocol
papers, and a protocol-classification-complete reference must still be eligible
for probe classification.

Both are abstract-only classifiers triggered by "inside corpus", so neither waits
on a PDF -- ZFIN mints a ZDB-PUB when its PubMed query finds the reference and
the ABC picks it up within a day, before students are given the paper.
"""

from typing import List, Tuple

PRE_INDEXING_PRIO_NEEDED = "ATP:0000306"
MOLECULAR_PROBE_CLASSIFICATION_NEEDED = "ATP:0000380"

# (ontology name, curie, every state of that tag's own workflow)
ZFIN_CORPUS_ENTRY_TAGS: List[Tuple[str, str, List[str]]] = [
    (
        "pre-indexing prioritization needed",
        PRE_INDEXING_PRIO_NEEDED,
        ["ATP:0000303",   # pre-indexing prioritization complete
         "ATP:0000304",   # pre-indexing prioritization failed
         "ATP:0000305",   # pre-indexing prioritization in progress
         "ATP:0000306"],  # pre-indexing prioritization needed
    ),
    (
        "molecular probe classification needed",
        MOLECULAR_PROBE_CLASSIFICATION_NEEDED,
        ["ATP:0000380",   # molecular probe classification needed
         "ATP:0000383",   # molecular probe classification in progress
         "ATP:0000385",   # molecular probe classification failed
         "ATP:0000387"],  # molecular probe classification complete
    ),
]
