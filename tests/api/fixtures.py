import pytest
from sqlalchemy import text

from agr_cognito_py import get_authentication_token, generate_headers


@pytest.fixture(scope="session")
def auth_headers():
    """Session-scoped fixture for Cognito admin token headers."""
    print("***** Generating Cognito admin token *****")
    yield generate_headers(get_authentication_token())


@pytest.fixture
def descriptor_rows(db):  # noqa: F811
    """The TESTMOD sample descriptor in A-team's tables (SCRUM-6720), plus an obsolete descriptor and an internal page the
    API's findForPublic left out. The tables are not in Base.metadata, so this cleans up."""
    statements = [
        "INSERT INTO resourcedescriptor (id, prefix, name, defaulturltemplate) "
        "VALUES (901, 'TESTMOD', 'Test Mod', 'http://test.org/[%s]')",
        "INSERT INTO resourcedescriptor (id, prefix, name, defaulturltemplate, obsolete) "
        "VALUES (902, 'GONE', 'Obsolete', 'http://gone.org/[%s]', true)",
        "INSERT INTO resourcedescriptor_synonyms VALUES (901, 'TMOD'), (901, 'TM')",
        "INSERT INTO resourcedescriptorpage (id, name, urltemplate, resourcedescriptor_id) VALUES "
        "(911, 'gene', 'http://test.org/gene/[%s]', 901), (912, 'homepage', 'http://test.org/', 901)",
        "INSERT INTO resourcedescriptorpage (id, name, urltemplate, resourcedescriptor_id, internal) "
        "VALUES (913, 'curator', 'http://test.org/curate/[%s]', 901, true)",
    ]
    cleanup = [
        "DELETE FROM resourcedescriptorpage WHERE resourcedescriptor_id IN (901, 902)",
        "DELETE FROM resourcedescriptor_synonyms WHERE resourcedescriptor_id IN (901, 902)",
        "DELETE FROM resourcedescriptor WHERE id IN (901, 902)",
    ]
    for statement in cleanup + statements:
        db.execute(text(statement))
    db.commit()
    yield
    for statement in cleanup:
        db.execute(text(statement))
    db.commit()
