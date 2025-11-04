from typing import Optional
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

# ----------------------------
# Pure-SQL helpers (no ORM imports)
# ----------------------------

# 1) textual users.id match (case-insensitive)
_SQL_MATCH_USERS_ID_TEXT = text(r"""
SELECT id
FROM users
WHERE lower(id) = lower(:identifier)
LIMIT 1
""")

# 2) email -> users.id
_SQL_MATCH_EMAIL = text(r"""
SELECT u.id
FROM users u
JOIN email e ON e.person_id = u.person_id
WHERE lower(e.email_address) = lower(:email)
LIMIT 1
""")

# 3) name (display_name full or initials+last) -> users.id
_SQL_MATCH_NAME = text(r"""
WITH parts AS (
  SELECT
    u.id,
    p.display_name,
    regexp_split_to_array(trim(p.display_name), '\s+') AS toks
  FROM users u
  JOIN person p ON p.person_id = u.person_id
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
LIMIT 1
""")


def create(db: Session, user_id: str, user_email: Optional[str] = None):
    """Create a user row (local import avoids circular imports)."""
    from agr_literature_service.api.models.user_model import UserModel
    user_obj = UserModel(
        id=user_id,
        automation_username=user_id,
        email=user_email,
        person_id=None,
    )
    db.add(user_obj)
    db.commit()
    db.refresh(user_obj)
    return user_obj


def map_to_user_id(identifier: str, db: Session) -> str:
    """
    Resolve an external identifier to the canonical users.id (TEXT).

    Resolution order:
      1) users.id (TEXT, case-insensitive)
      2) if contains '@': person.email -> users.id
      3) else: match normalized person.display_name (full or initials+last)

    Returns: users.id (text)
    Raises:  HTTPException(422) if empty or not found.
    """
    if not identifier or not identifier.strip():
        raise HTTPException(status_code=422, detail="created_by/updated_by is empty")

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
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unknown user identifier: {identifier!r}. "
                "Expected users.id, an email address, a full display name, "
                "or initials+last (e.g., 'G. D. Smith', 'G. Williams')."
            ),
        )
    return users_id
