"""SCRUM-6720: SQL that ORM comparisons on curie attributes (ontology_term_model.ontology_curie) produce."""
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import aliased

from agr_literature_service.api.models import IndexingPriorityModel, WorkflowTagTopicModel

from .fixtures import db  # noqa: F401


def _sql(statement) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))


def test_curie_literal_compares_the_id_column():
    sql = _sql(select(WorkflowTagTopicModel.workflow_tag_topic_id).where(WorkflowTagTopicModel.topic == 'ATP:1'))
    assert 'workflow_tag_topic.topic_id = (SELECT ontologyterm.id' in sql


def test_two_curie_attributes_compare_their_id_columns():
    sql = _sql(select(WorkflowTagTopicModel.workflow_tag_topic_id).where(
        WorkflowTagTopicModel.workflow_tag == WorkflowTagTopicModel.topic))
    assert 'workflow_tag_topic.workflow_tag_id = workflow_tag_topic.topic_id' in sql
    sql = _sql(select(WorkflowTagTopicModel.workflow_tag_topic_id).where(
        WorkflowTagTopicModel.workflow_tag != IndexingPriorityModel.predicted_indexing_priority))
    assert 'workflow_tag_topic.workflow_tag_id != indexing_priority.predicted_indexing_priority_id' in sql


def test_aliased_entity_compares_the_aliased_id_column():
    wtt = aliased(WorkflowTagTopicModel, name='wtt')
    sql = _sql(select(wtt.workflow_tag_topic_id).where(wtt.topic == 'ATP:1', wtt.workflow_tag.in_(['ATP:2'])))
    assert 'wtt.topic_id = (SELECT ontologyterm.id' in sql
    assert 'wtt.workflow_tag_id IN (SELECT ontologyterm.id' in sql
    assert 'workflow_tag_topic.' not in sql.split('FROM', 1)[1].split('WHERE', 1)[1]


def test_none_compares_the_id_column_with_null():
    sql = _sql(select(WorkflowTagTopicModel.workflow_tag_topic_id).where(WorkflowTagTopicModel.topic.is_(None)))
    assert 'workflow_tag_topic.topic_id IS NULL' in sql


def test_workflow_tag_joins_its_topic_row(db):  # noqa: F811
    """The join get_jobs uses (workflow_tag_crud.get_jobs): a workflow tag to the topic row of its
    curie, then a filter on the topic curie."""
    from agr_literature_service.api.models import ModModel, ReferenceModel, WorkflowTagModel
    mod = db.query(ModModel).first() or ModModel(abbreviation='WB', short_name='WB', full_name='WormBase')
    reference = ReferenceModel(curie='AGRKB:SCRUM6720-wtt', category='research_article')
    db.add_all([mod, reference])
    db.flush()
    db.add(WorkflowTagTopicModel(workflow_tag='ATP:0000141', topic='ATP:0000009'))
    db.add(WorkflowTagModel(reference_id=reference.reference_id, mod_id=mod.mod_id, workflow_tag_id='ATP:0000141'))
    db.commit()
    rows = (db.query(WorkflowTagModel.workflow_tag_id, WorkflowTagTopicModel.topic)
            .outerjoin(WorkflowTagTopicModel, WorkflowTagModel.workflow_tag_id == WorkflowTagTopicModel.workflow_tag)
            .filter(WorkflowTagModel.reference_id == reference.reference_id,
                    WorkflowTagTopicModel.topic == 'ATP:0000009')
            .all())
    assert [(row.workflow_tag_id, row.topic) for row in rows] == [('ATP:0000141', 'ATP:0000009')]


def test_notin_alias_compares_the_id_column():
    sql = _sql(select(WorkflowTagTopicModel.workflow_tag_topic_id).where(WorkflowTagTopicModel.topic.notin_(['ATP:1'])))
    assert 'workflow_tag_topic.topic_id NOT IN (SELECT ontologyterm.id' in sql
