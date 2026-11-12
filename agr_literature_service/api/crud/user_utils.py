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

# An agent's label is its automation_username, or the curie of its person (the
# legacy users.id); it is what created_by / updated_by hold in the API.

# 1) textual label match (case-insensitive)
_SQL_MATCH_USERS_ID_TEXT = text(r"""
SELECT COALESCE(a.automation_username, p.curie)
FROM agent a
LEFT JOIN person p ON p.id = a.person_id
WHERE lower(a.automation_username) = lower(:identifier)
   OR lower(p.curie) = lower(:identifier)
ORDER BY a.id
LIMIT 1
""")

# 2) email -> label
_SQL_MATCH_EMAIL = text(r"""
SELECT p.curie
FROM agent a
JOIN person p ON p.id = a.person_id
JOIN person_email e ON e.person_id = p.id
WHERE lower(e.email_address) = lower(:email)
LIMIT 1
""")

# 3) name (display_name full or initials+last) -> label. The same name forms
# also match persons with no agent yet (_SQL_PERSONS_BY_NAME); __PERSONS__ and
# __LIMIT__ are filled in below.
_NAME_MATCH_SQL = r"""
WITH parts AS (
  SELECT
    p.curie AS id,
    p.display_name,
    regexp_split_to_array(trim(p.display_name), '\s+') AS toks
  __PERSONS__
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
__LIMIT__
"""
_SQL_MATCH_NAME = text(_NAME_MATCH_SQL
                       .replace("__PERSONS__", "FROM agent a\n  JOIN person p ON p.id = a.person_id")
                       .replace("__LIMIT__", "LIMIT 1"))
# every person, agent or not, whose name matches: more than one is ambiguous
_SQL_PERSONS_BY_NAME = text(_NAME_MATCH_SQL
                            .replace("__PERSONS__", "FROM person p")
                            .replace("__LIMIT__", "ORDER BY nf.id\nLIMIT 5"))

_SQL_AUTOMATION_AGENT = text(r"""
SELECT id, automation_username FROM agent
WHERE lower(automation_username) = lower(:label)
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

_SQL_PERSON_AGENT = text(r"""
SELECT a.id FROM agent a JOIN person p ON p.id = a.person_id
WHERE p.curie = :curie
ORDER BY a.id
LIMIT 1
""")

_SQL_INSERT_PERSON_AGENT = text(r"""
INSERT INTO agent (person_id, automation_username)
SELECT id, NULL FROM person WHERE curie = :curie
RETURNING id
""")

# the person a (non-obsolete) person cross reference points at; the curie is unique among those
_SQL_PERSON_BY_XREF = text(r"""
SELECT p.curie
FROM person_cross_reference x
JOIN person p ON p.id = x.person_id
WHERE lower(x.curie) = lower(:curie) AND NOT x.obsolete
LIMIT 1
""")

# an automation agent standing in for a person becomes an agent of that person;
# the records it created or updated then show the person's curie
_SQL_CLAIM_PLACEHOLDER = text(r"""
UPDATE agent
SET person_id = (SELECT id FROM person WHERE curie = :curie), automation_username = NULL
WHERE id = :agent_id
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
        return existing.automation_username
    logger.warning(f"{placeholder} matches no person in ABC; recording it under a placeholder automation "
                   "agent until the WB persons are imported")
    return placeholder


def map_to_user_id(identifier: str, db: Session) -> str:
    """
    Resolve an external identifier to the label of an agent (automation name or person curie).

    The kind of identifier is decided by its shape (SCRUM-6729):
      1) a person: an AGRKB person curie, a person cross reference curie
         (WB:WBPerson1, ORCID:..., SGD:Colleague_...) or a bare WBPerson id
         -> the person's curie. A person with no agent gets one; a WB person's
         placeholder automation agent becomes theirs (see _person_agent).
      2) a WB person id that matches no person -> its placeholder automation
         agent, reused case-insensitively; a new one only for a new id.
      3) any other curie-shaped identifier -> 422: it can only be a person, and
         it would otherwise become a fake automation agent.
      4) contains '@': the person with that email -> curie; 422 if none, or if
         the email is shared and none of them has an agent.
      5) an existing agent label (case-insensitive)
      6) a person's display name (full or initials+last)
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
        _person_agent(db, person, placeholder=placeholder)
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
        users_id = db.execute(_SQL_MATCH_EMAIL, {"email": ident}).scalar_one_or_none()
        if users_id:
            return users_id
        curies = list(db.execute(_SQL_PERSONS_BY_EMAIL, {"email": ident}).scalars())
        if not curies:
            raise HTTPException(status_code=422, detail=f"Unknown email: {identifier!r}.")
        if len(curies) > 1:
            raise HTTPException(
                status_code=422,
                detail=(f"Email {identifier!r} belongs to more than one person ({', '.join(curies)}); "
                        "use the person's AGRKB curie."),
            )
        _person_agent(db, curies[0])
        return curies[0]

    # 5) textual users.id
    users_id = db.execute(_SQL_MATCH_USERS_ID_TEXT, {"identifier": ident}).scalar_one_or_none()
    if users_id:
        return users_id

    # 6) name (full display_name or initials+last)
    users_id = db.execute(_SQL_MATCH_NAME, {"id_raw": ident}).scalar_one_or_none()
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


def resolve_named_agent(identifier: str, db: Session) -> Tuple[int, str]:
    """
    Resolve a curator a service account names as the author of a change
    (SCRUM-6819) to (agent.id, label).

    Accepts an automation agent's name, or a person's curie (AGRKB ID), email
    or display name (full, or initials+last). Unlike map_to_existing_user_id
    the person need not have an agent yet -- most persons have never logged
    in to ABC -- and one is created for them; unlike map_to_user_id nothing is
    created for a name that matches no person, so a typo cannot record the
    change under a made-up name.

    Raises HTTPException(422) when the identifier is empty, matches no one, or
    (an email or a name) matches more than one person.
    """
    ident = (identifier or "").strip()
    if not ident:
        raise HTTPException(status_code=422, detail="updated_by cannot be empty")
    # punctuation alone normalizes to '' and would match a person with no name
    if not any(c.isalnum() for c in ident):
        raise HTTPException(status_code=422, detail=f"Unknown updated_by {identifier!r}: no letters or digits")

    # a person curie or cross reference first: a WB person found here wins over
    # their placeholder automation agent (SCRUM-6729)
    curie = person_for_identifier(db, ident)
    if curie:
        return _person_agent(db, curie, placeholder=wb_person_placeholder(ident)), curie

    automation = db.execute(_SQL_AUTOMATION_AGENT, {"label": ident}).fetchone()
    if automation:
        return automation.id, automation.automation_username

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
            detail=(f"Unknown updated_by {identifier!r}: expected a person's AGRKB ID, "
                    "email or full name"),
        )
    if len(curies) > 1:
        raise HTTPException(
            status_code=422,
            detail=(f"updated_by {identifier!r} matches more than one person "
                    f"({', '.join(curies)}); use the person's AGRKB ID or email"),
        )
    return _person_agent(db, curies[0]), curies[0]


def claim_wb_placeholder(db: Session, placeholder: str) -> Optional[str]:
    """Hand a WB placeholder automation agent to its person if ABC now has them
    (a WB:WBPerson... cross reference); returns the person's curie, or None
    when the person is not in ABC yet. Not committed."""
    person = person_for_identifier(db, placeholder)
    if person:
        _person_agent(db, person, placeholder=wb_person_placeholder(placeholder))
    return person


def _placeholder_agent_id(db: Session, placeholder: Optional[str]) -> Optional[int]:
    if placeholder is None:
        return None
    row = db.execute(_SQL_AUTOMATION_AGENT, {"label": placeholder}).fetchone()
    return row.id if row else None


def _person_agent(db: Session, curie: str, placeholder: Optional[str] = None) -> int:
    """agent.id of the person, creating their agent if they have none. A
    transaction-scoped advisory lock on the curie keeps two concurrent
    requests from creating two agents for the same person.

    placeholder is the name of the automation agent that stood in for a WB
    person before they were in ABC (WBPerson1843). If it exists it is claimed
    for the person (person_id set, name cleared), so its records show the
    person and no agent is created: it becomes the person's agent, or, when
    the person already has one, a second agent of the same person
    (SCRUM-6729). Only agent rows change, never the records."""
    agent_id = db.execute(_SQL_PERSON_AGENT, {"curie": curie}).scalar()
    if agent_id is not None and _placeholder_agent_id(db, placeholder) is None:
        return agent_id
    db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"person_agent:{curie}"})
    agent_id = db.execute(_SQL_PERSON_AGENT, {"curie": curie}).scalar()
    placeholder_id = _placeholder_agent_id(db, placeholder)
    if placeholder_id is not None:
        db.execute(_SQL_CLAIM_PLACEHOLDER, {"curie": curie, "agent_id": placeholder_id})
        logger.info(f"agent {placeholder_id} ({placeholder}) now belongs to person {curie}")
        if agent_id is None:
            agent_id = placeholder_id
    if agent_id is None:
        agent_id = db.execute(_SQL_INSERT_PERSON_AGENT, {"curie": curie}).scalar()
    return agent_id


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
