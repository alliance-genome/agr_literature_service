from sqlalchemy import text


get_most_current_email = r"""
CREATE OR REPLACE FUNCTION get_most_current_email(p_person_id INTEGER)
RETURNS TEXT AS $$
  SELECT email_address
  FROM person_email
  WHERE person_id = p_person_id
    AND date_made_old_email IS NULL
  ORDER BY COALESCE(date_updated, date_created) DESC,
           email_id DESC
  LIMIT 1;
$$ LANGUAGE SQL STABLE;
"""


def add_person_email_functions(db_session):
    db_session.execute(text(get_most_current_email))
    db_session.commit()
