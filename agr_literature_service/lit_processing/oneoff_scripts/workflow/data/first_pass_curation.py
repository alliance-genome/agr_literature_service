"""
first pass curation (ATP:0000329)
    first pass curation needed (ATP:0000331)
    first pass curation in progress (ATP:0000332)
    first pass curation blocked (ATP:0000333)
    first pass curation TBD (ATP:0000371)
    first pass curation finished (ATP:0000330)

SCRUM-5478 - FlyBase only.

First pass curation is a manually applied workflow tag, so a curator can move a paper
from any state to any other state (all-to-all manual transitions among the five status
states above). Those transitions are seeded directly into the workflow_transition table
by populate_workflow_transition_first_pass_curation.py, which imports the constants below.

Unlike the other workflow data files, this module has no ``get_data`` function: the
database is the source of truth and the rows are populated by ATP id, not loaded from a
name-based data file via transitions_add.py.

The automated "first pass curation TBD" (ATP:0000371) tag is NOT a transition row. Entity
extraction (the last pre-curation step) reaches "complete" via the subtask roll-up in
sub_task_complete, which sets the main tag directly rather than via a transition; that
roll-up calls ``set_first_pass_curation_tbd`` once entity extraction complete
(ATP:0000174), reference/topic classification complete (ATP:0000169) and curation
classification complete (ATP:0000312) all exist for the reference+MOD.
"""

# The five applyable first pass curation status states (FlyBase only).
FIRST_PASS_CURATION_STATE_ATPS = [
    'ATP:0000331',  # first pass curation needed
    'ATP:0000332',  # first pass curation in progress
    'ATP:0000333',  # first pass curation blocked
    'ATP:0000371',  # first pass curation TBD
    'ATP:0000330',  # first pass curation finished
]

FIRST_PASS_CURATION_MOD = 'FB'
