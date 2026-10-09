"""SCRUM-6448: the "author review" workflow the PubMed update sets for MODs whose
curators edit author lists by hand.

The PubMed update never overwrites the authors of a reference a curator has
edited (``db_write_utils._reference_touched_by_curator``). Instead, for each MOD
listed here whose corpus holds the reference, it flags the paper for review:

* PubMed's authors differ from ABC's -> "author review needed". A reference with
  no tag yet gets it; one whose review is complete is reopened, since PubMed
  changed after that review. "In progress" and "blocked" are left alone because
  a curator is working on them.
* PubMed's authors match ABC's and the reference is "author review needed" ->
  "author review complete", stamped with the updating script's user.

Only the author name, order and ORCID count as a difference
(``author.authors_lists_match_for_review``).

Curators move papers between the four states by hand, through the all-to-all
manual transitions added by
``oneoff_scripts/populate_workflow_transition_author_review.py``.
"""

from typing import Dict, List

AUTHOR_REVIEW = "ATP:0000388"
AUTHOR_REVIEW_NEEDED = "ATP:0000389"
AUTHOR_REVIEW_IN_PROGRESS = "ATP:0000390"
AUTHOR_REVIEW_BLOCKED = "ATP:0000391"
AUTHOR_REVIEW_COMPLETE = "ATP:0000392"

# Every state of the author review workflow: a (reference, mod) holds at most one.
AUTHOR_REVIEW_STATES: List[str] = [
    AUTHOR_REVIEW_NEEDED,
    AUTHOR_REVIEW_IN_PROGRESS,
    AUTHOR_REVIEW_BLOCKED,
    AUTHOR_REVIEW_COMPLETE,
]

# The ontology names of those states, used by the transitions script to resolve
# them from the A-Team ontology with the ids above kept as fallbacks.
AUTHOR_REVIEW_STATE_NAME_TO_ATP: Dict[str, str] = {
    "author review needed": AUTHOR_REVIEW_NEEDED,
    "author review in progress": AUTHOR_REVIEW_IN_PROGRESS,
    "author review blocked": AUTHOR_REVIEW_BLOCKED,
    "author review complete": AUTHOR_REVIEW_COMPLETE,
}

# The MODs whose corpus references get author review tags.
AUTHOR_REVIEW_MODS: List[str] = ["WB"]
