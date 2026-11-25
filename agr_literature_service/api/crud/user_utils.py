import logging
import re
from typing import List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# A person identifier is classified by its shape (SCRUM-6729):
#   - PREFIX:local id is a person curie (AGRKB) or a person cross reference curie
#     (WB:WBPerson1, ORCID:..., SGD:Colleague_..., ZFIN:ZDB-PERS-..., XenBase:...);
#   - a WormBase person id is also accepted bare (WBPerson1843), as the Caltech
#     pipelines send it.
# No automation agent name contains ':', so a curie-shaped identifier never
# means an automation agent.
_CURIE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*:\S+$")
_WB_PERSON_RE = re.compile(r"^(?:WB:)?WBPerson(\d+)$", re.IGNORECASE)

# ----------------------------
# Pure-SQL helpers (no ORM imports)
# ----------------------------

# An agent's label is the curie of its person, or else its display_name (an
# automation's name); it is what created_by / updated_by hold in the API. A person
# is an agent (person.id = agent.id, SCRUM-6995).

# 1) textual label match (case-insensitive)
_SQL_MATCH_USERS_ID_TEXT = text(r"""
SELECT label FROM (
    SELECT a.id, a.display_name AS label FROM agent a
    WHERE a.agent_type = 'automation' AND lower(a.display_name) = lower(:identifier)
    UNION ALL
    SELECT p.id, p.curie FROM person p WHERE lower(p.curie) = lower(:identifier)
) AS labelled
ORDER BY id
LIMIT 1
""")

# 2) name (display_name full or initials+last) -> person curies, at most 5:
# more than one is ambiguous
_SQL_PERSONS_BY_NAME = text(r"""
WITH parts AS (
  SELECT
    p.curie AS id,
    a.display_name,
    regexp_split_to_array(trim(a.display_name), '\s+') AS toks
  FROM person p
  JOIN agent a ON a.id = p.id
),
name_forms AS (
  SELECT
    id,

    -- Full name: lower THEN collapse spaces (preserve letters)
    regexp_replace(lower(display_name), '\s+', ' ', 'g') AS full_space_norm,

    -- LAST name: lower THEN strip punctuation
    regexp_replace(lower(toks[array_length(toks, 1)]), '[^a-z0-9]', '', 'g') AS last_norm,

    -- ALL initials (concat): lower THEN strip punctuation
    regexp_replace(
      lower(
        COALESCE(
          (SELECT string_agg(left(tok, 1), '' ORDER BY ord)
           FROM unnest(toks) WITH ORDINALITY AS t(tok, ord)
           WHERE ord < array_length(toks, 1)),
          ''
        )
      ),
      '[^a-z0-9]', '', 'g'
    ) AS initials_concat_norm,

    -- ALL initials (spaced): lower THEN strip non-alnum/space
    regexp_replace(
      lower(
        COALESCE(
          (SELECT string_agg(left(tok, 1), ' ' ORDER BY ord)
           FROM unnest(toks) WITH ORDINALITY AS t(tok, ord)
           WHERE ord < array_length(toks, 1)),
          ''
        )
      ),
      '[^a-z0-9 ]', '', 'g'
    ) AS initials_spaced_norm,

    -- FIRST initial only: lower THEN strip non-alnum
    regexp_replace(lower(left(toks[1], 1)), '[^a-z0-9]', '', 'g') AS first_initial_norm
  FROM parts
),
id_norm AS (
  SELECT
    -- Do lower() FIRST, then strip, to keep uppercase letters
    regexp_replace(lower(btrim(:id_raw)), '\s+', ' ', 'g') AS id_space_norm,
    regexp_replace(
      regexp_replace(lower(btrim(:id_raw)), '\s+', ' ', 'g'),
      '[^a-z0-9 ]', '', 'g'
    ) AS id_alnum_norm,
    regexp_replace(
      regexp_replace(lower(btrim(:id_raw)), '\s+', '', 'g'),
      '[^a-z0-9]', '', 'g'
    ) AS id_compact_norm
)
SELECT nf.id
FROM name_forms nf, id_norm i
WHERE
      nf.full_space_norm = i.id_space_norm
   OR concat(nf.initials_spaced_norm, ' ', nf.last_norm) = i.id_alnum_norm
   OR concat(nf.initials_concat_norm, nf.last_norm) = i.id_compact_norm
   OR concat(nf.first_initial_norm, ' ', nf.last_norm) = i.id_alnum_norm
   OR concat(nf.first_initial_norm, nf.last_norm) = i.id_compact_norm
ORDER BY nf.id
LIMIT 5
""")

_SQL_AUTOMATION_AGENT = text(r"""
SELECT id, display_name FROM agent
WHERE agent_type = 'automation' AND lower(display_name) = lower(:label)
ORDER BY id
LIMIT 1
""")

_SQL_PERSON_BY_CURIE = text(r"""
SELECT curie FROM person WHERE lower(curie) = lower(:curie)
""")

_SQL_PERSONS_BY_EMAIL = text(r"""
SELECT DISTINCT p.curie
FROM person p
JOIN person_email e ON e.person_id = p.id
WHERE lower(e.email_address) = lower(:email)
ORDER BY p.curie
LIMIT 5
""")

_SQL_PERSON_ID = text(r"""
SELECT id FROM person WHERE curie = :curie
""")

# the person a (non-obsolete) person cross reference points at; the curie is unique among those
_SQL_PERSON_BY_XREF = text(r"""
SELECT p.curie
FROM person_cross_reference x
JOIN person p ON p.id = x.person_id
WHERE lower(x.curie) = lower(:curie) AND x.obsolete IS FALSE
LIMIT 1
""")


def wb_person_placeholder(identifier: str) -> Optional[str]:
    """The canonical bare WormBase person id (WBPerson1843) for a WB person id,
    bare or prefixed and in any case; None for anything else. It is also the
    name of the placeholder automation agent that stands in for the person
    until the WB persons are in ABC."""
    match = _WB_PERSON_RE.match(identifier.strip())
    return f"WBPerson{match.group(1)}" if match else None


def person_for_identifier(db: Session, identifier: str) -> Optional[str]:
    """The curie of the person an AGRKB person curie or a person cross reference
    names (a bare WBPerson id counts as WB:WBPerson...); None if it names no
    person or is not curie-shaped."""
    ident = identifier.strip()
    placeholder = wb_person_placeholder(ident)
    if placeholder:
        ident = f"WB:{placeholder}"
    elif not _CURIE_RE.match(ident):
        return None
    return (db.execute(_SQL_PERSON_BY_CURIE, {"curie": ident}).scalar()
            or db.execute(_SQL_PERSON_BY_XREF, {"curie": ident}).scalar())


def _resolve_wb_placeholder(db: Session, placeholder: str) -> str:
    """Label for a WB person id that matches no person yet: its existing
    placeholder automation agent (matched case-insensitively, so it is never
    duplicated), else the bare id, which the audit listener creates."""
    existing = db.execute(_SQL_AUTOMATION_AGENT, {"label": placeholder}).fetchone()
    if existing:
        return existing.display_name
    logger.warning(f"{placeholder} matches no person in ABC; recording it under a placeholder automation "
                   "agent until the WB persons are imported")
    return placeholder


def map_to_user_id(identifier: str, db: Session) -> str:
    """
    Resolve an external identifier to the label of an agent (automation name or person curie).

    The kind of identifier is decided by its shape (SCRUM-6729):
      1) a person: an AGRKB person curie, a person cross reference curie
         (WB:WBPerson1, ORCID:..., SGD:Colleague_...) or a bare WBPerson id
         -> the person's curie (a person is an agent, SCRUM-6995). A WB person's
         placeholder automation agent keeps its records until
         claim_wb_placeholder merges it into the person.
      2) a WB person id that matches no person -> its placeholder automation
         agent, reused case-insensitively; a new one only for a new id.
      3) any other curie-shaped identifier -> 422: it can only be a person, and
         it would otherwise become a fake automation agent.
      4) contains '@': the person with that email -> curie; 422 if none, or if
         the email is shared by more than one person.
      5) an existing agent label (case-insensitive)
      6) the one person with that display name (full or initials+last); a name
         several persons share is not a match
      7) else echoed back: the audit listener creates an automation agent, as
         a new script's name does.

    Returns: the agent label (text)
    Raises:  HTTPException(422) for an unknown person curie or email.
    """
    if not identifier or not identifier.strip():
        return ""

    ident = identifier.strip()

    # 1) a person, by AGRKB curie or cross reference
    placeholder = wb_person_placeholder(ident)
    person = person_for_identifier(db, ident)
    if person:
        return person

    # 2) a WB person not in ABC yet
    if placeholder:
        return _resolve_wb_placeholder(db, placeholder)

    # 3) an unknown person curie
    if _CURIE_RE.match(ident):
        raise HTTPException(
            status_code=422,
            detail=(f"Unknown person identifier: {identifier!r} is neither an AGRKB person curie "
                    "nor a person cross reference in ABC."),
        )

    # 4) email
    if "@" in ident:
        curies = list(db.execute(_SQL_PERSONS_BY_EMAIL, {"email": ident}).scalars())
        if not curies:
            raise HTTPException(status_code=422, detail=f"Unknown email: {identifier!r}.")
        if len(curies) > 1:
            raise HTTPException(
                status_code=422,
                detail=(f"Email {identifier!r} belongs to more than one person ({', '.join(curies)}); "
                        "use the person's AGRKB curie."),
            )
        return curies[0]

    # 5) textual users.id
    users_id = db.execute(_SQL_MATCH_USERS_ID_TEXT, {"identifier": ident}).scalar_one_or_none()
    if users_id:
        return users_id

    # 6) name (full display_name or initials+last), when only one person has it
    named = list(db.execute(_SQL_PERSONS_BY_NAME, {"id_raw": ident}).scalars())
    users_id = named[0] if len(named) == 1 else None
    if not users_id:
        """
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unknown user identifier: {identifier!r}. "
                "Expected users.id, an email address, a full display name, "
                "or initials+last (e.g., 'G. D. Smith', 'G. Williams')."
            ),
        )
        """
        return ident
    return users_id


def resolve_named_agent(identifier: str, db: Session, field: str = "updated_by") -> Tuple[int, str]:
    """
    Resolve a curator a service account names as the author of a change
    (SCRUM-6819) to (agent.id, label).

    Accepts an automation agent's name, or a person's curie (AGRKB ID), email
    or display name (full, or initials+last); a person is an agent (SCRUM-6995).
    Unlike map_to_user_id nothing is created for a name that matches no person,
    so a typo cannot record the change under a made-up name.

    Raises HTTPException(422) when the identifier is empty, matches no one, or
    (an email or a name) matches more than one person. ``field`` is the request
    field the identifier came from, for those messages.
    """
    ident = (identifier or "").strip()
    if not ident:
        raise HTTPException(status_code=422, detail=f"{field} cannot be empty")
    # punctuation alone normalizes to '' and would match a person with no name
    if not any(c.isalnum() for c in ident):
        raise HTTPException(status_code=422, detail=f"Unknown {field} {identifier!r}: no letters or digits")

    # a person curie or cross reference first: a WB person found here wins over
    # their placeholder automation agent (SCRUM-6729)
    curie = person_for_identifier(db, ident)
    if curie:
        return _person_agent(db, curie), curie

    automation = db.execute(_SQL_AUTOMATION_AGENT, {"label": ident}).fetchone()
    if automation:
        return automation.id, automation.display_name

    curies: List[str] = []
    if _CURIE_RE.match(ident):
        pass  # an unknown person curie or cross reference
    elif "@" in ident:
        curies = list(db.execute(_SQL_PERSONS_BY_EMAIL, {"email": ident}).scalars())
    else:
        curies = list(db.execute(_SQL_PERSONS_BY_NAME, {"id_raw": ident}).scalars())
    if not curies:
        raise HTTPException(
            status_code=422,
            detail=(f"Unknown {field} {identifier!r}: expected a person's AGRKB ID, "
                    "email or full name"),
        )
    if len(curies) > 1:
        raise HTTPException(
            status_code=422,
            detail=(f"{field} {identifier!r} matches more than one person "
                    f"({', '.join(curies)}); use the person's AGRKB ID or email"),
        )
    return _person_agent(db, curies[0]), curies[0]


def resolve_named_person(identifier: str, db: Session, field: str = "created_by") -> Tuple[int, str]:
    """
    Resolve the curator a verdict is recorded for (SCRUM-6853) to (agent.id,
    person curie). Stricter than resolve_named_agent: only a person's AGRKB ID
    or email is accepted, never an automation agent (default_user, a pipeline)
    and never a display name, which can match the wrong person.

    Raises HTTPException(422) when the identifier names no person, an
    automation agent, or (an email) more than one person.
    """
    ident = (identifier or "").strip()
    if not ident:
        raise HTTPException(status_code=422, detail=f"{field} cannot be empty")
    curie = db.execute(_SQL_PERSON_BY_CURIE, {"curie": ident}).scalar()
    if curie:
        return _person_agent(db, curie), curie
    if "@" in ident:
        curies = list(db.execute(_SQL_PERSONS_BY_EMAIL, {"email": ident}).scalars())
        if len(curies) == 1:
            return _person_agent(db, curies[0]), curies[0]
        if len(curies) > 1:
            raise HTTPException(
                status_code=422,
                detail=(f"{field} {identifier!r} matches more than one person "
                        f"({', '.join(curies)}); use the person's AGRKB ID"),
            )
    if db.execute(_SQL_AUTOMATION_AGENT, {"label": ident}).fetchone():
        raise HTTPException(status_code=422,
                            detail=f"{field} {identifier!r} names an automation agent, not a curator")
    raise HTTPException(status_code=422,
                        detail=f"Unknown {field} {identifier!r}: expected a person's AGRKB ID or email")


def claim_wb_placeholder(db: Session, placeholder: str) -> Optional[str]:
    """Merge a WB placeholder automation agent (WBPerson1843) into its person if ABC
    now has them (a WB:WBPerson... cross reference): the records it created or updated
    then show the person's curie, and the placeholder agent is gone (SCRUM-6995).
    Returns the person's curie, or None when the person is not in ABC yet. Rewrites
    every audited table (see merge_agent_into): for scripts, not API requests. Not
    committed."""
    from agr_literature_service.api.crud.agent_crud import merge_agent_into

    person = person_for_identifier(db, placeholder)
    if person is None:
        return None
    canonical = wb_person_placeholder(placeholder)
    agent = db.execute(_SQL_AUTOMATION_AGENT, {"label": canonical}).fetchone() if canonical else None
    if agent is not None:
        counts = merge_agent_into(db, agent.id, _person_agent(db, person))
        logger.info(f"placeholder agent {agent.id} ({agent.display_name}) merged into person {person}: {counts}")
    return person


def _person_agent(db: Session, curie: str) -> int:
    """agent.id of the person with this curie: the person's own id (SCRUM-6995)."""
    return db.execute(_SQL_PERSON_ID, {"curie": curie}).scalar_one()


def map_to_existing_user_id(identifier: str, db: Session) -> Optional[str]:
    """
    Like map_to_user_id, but returns None when the identifier does not resolve
    to an EXISTING users row.

    map_to_user_id deliberately echoes unknown identifiers back so admin-token
    loads can auto-create their users rows (see audited_model auto-create).
    Callers that must NOT mint users from caller-supplied audit fields -- e.g.
    PATCH created_by/updated_by, where the UI used to send its auth token's
    Cognito sub (SCRUM-6459) -- use this and drop the field when it returns
    None, letting the audited-model listener stamp the authenticated user.

    Unlike map_to_user_id, an unknown EMAIL or person curie also returns None
    here (there it raises 422) -- all no-match outcomes look the same to a
    caller of this helper. A real person named by curie, cross reference or
    email does get an agent if they have none: that is the person, not a
    made-up user.
    """
    try:
        mapped = map_to_user_id(identifier, db)
    except HTTPException:
        return None
    if not mapped:
        return None
    return db.execute(_SQL_MATCH_USERS_ID_TEXT, {"identifier": mapped}).scalar_one_or_none()
