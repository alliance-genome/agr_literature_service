from typing import Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

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


def map_to_user_id(identifier: str, db: Session) -> str:
    """
    Resolve an external identifier to the label of an agent (automation name or person curie).

    Resolution order:
      1) agent label (case-insensitive)
      2) if contains '@': person.email -> label
      3) else: match normalized person.display_name (full or initials+last)

    Returns: the agent label (text)
    Raises:  HTTPException(422) if empty or not found.
    """
    if not identifier or not identifier.strip():
        return ""

    ident = identifier.strip()

    # 1) textual users.id
    users_id = db.execute(_SQL_MATCH_USERS_ID_TEXT, {"identifier": ident}).scalar_one_or_none()
    if users_id:
        return users_id

    # 2) email
    if "@" in ident:
        users_id = db.execute(_SQL_MATCH_EMAIL, {"email": ident}).scalar_one_or_none()
        if not users_id:
            raise HTTPException(status_code=422, detail=f"Unknown email: {identifier!r}.")
        return users_id

    # 3) name (full display_name or initials+last)
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


def resolve_named_agent(identifier: str, db: Session, field: str = "updated_by") -> Tuple[int, str]:
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
    (an email or a name) matches more than one person. ``field`` is the request
    field the identifier came from, for those messages.
    """
    ident = (identifier or "").strip()
    if not ident:
        raise HTTPException(status_code=422, detail=f"{field} cannot be empty")
    # punctuation alone normalizes to '' and would match a person with no name
    if not any(c.isalnum() for c in ident):
        raise HTTPException(status_code=422, detail=f"Unknown {field} {identifier!r}: no letters or digits")

    automation = db.execute(_SQL_AUTOMATION_AGENT, {"label": ident}).fetchone()
    if automation:
        return automation.id, automation.automation_username

    curie = db.execute(_SQL_PERSON_BY_CURIE, {"curie": ident}).scalar()
    if curie:
        curies = [curie]
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


def _person_agent(db: Session, curie: str) -> int:
    """agent.id of the person, creating their agent if they have none. A
    transaction-scoped advisory lock on the curie keeps two concurrent
    requests from creating two agents for the same person."""
    agent_id = db.execute(_SQL_PERSON_AGENT, {"curie": curie}).scalar()
    if agent_id is not None:
        return agent_id
    db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"person_agent:{curie}"})
    agent_id = db.execute(_SQL_PERSON_AGENT, {"curie": curie}).scalar()
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

    Unlike map_to_user_id, an unknown EMAIL also returns None here (there it
    raises 422) -- all no-match outcomes look the same to a caller of this
    helper.
    """
    try:
        mapped = map_to_user_id(identifier, db)
    except HTTPException:
        return None
    if not mapped:
        return None
    return db.execute(_SQL_MATCH_USERS_ID_TEXT, {"identifier": mapped}).scalar_one_or_none()
