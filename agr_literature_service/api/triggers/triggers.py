from agr_literature_service.api.triggers.citation_sql_func_triggers import add_citation_methods
from agr_literature_service.api.triggers.author_sql_func_triggers import add_author_triggers
from agr_literature_service.api.triggers.reference_sql_func_triggers import add_reference_triggers
from agr_literature_service.api.triggers.resource_sql_func_triggers import add_resource_triggers


def add_sql_triggers_functions(db_session):
    add_citation_methods(db_session)
    add_author_triggers(db_session)
    add_reference_triggers(db_session)
    add_resource_triggers(db_session)
