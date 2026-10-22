"""SCRUM-6720 Phase 4: trigger-maintained curie mirrors, which the search pipeline reads."""
import re
from pathlib import Path

from sqlalchemy import text

from agr_literature_service.api.models import (ModModel, ReferenceModel, TagSourceModel, TopicEntityTagModel)
from agr_literature_service.api.triggers.curie_mirror_sql_func_triggers import CURIE_MIRRORS, add_curie_mirror_triggers

from .fixtures import db  # noqa: F401

KSQL = (Path(__file__).resolve().parent.parent / "debezium" / "ksql_queries.ksql").read_text()


def _mirrors(db, table, row_id):  # noqa: F811
    columns = ", ".join(curie for _id, curie in CURIE_MIRRORS[table])
    return dict(db.execute(text(f"SELECT {columns} FROM {table} WHERE id = :id"), {"id": row_id}).mappings().one())


def _tag(db):  # noqa: F811
    mod = db.query(ModModel).first() or ModModel(abbreviation='WB', short_name='WB', full_name='WormBase')
    reference = ReferenceModel(curie='AGRKB:672000000000004', category='research_article')
    db.add_all([mod, reference])
    db.flush()
    source = TagSourceModel(source_evidence_assertion='ECO:0006155', source_method='abc_literature_system',
                            description='curator', data_provider='WB', secondary_data_provider_id=mod.mod_id)
    db.add(source)
    db.flush()
    tag = TopicEntityTagModel(reference_id=reference.reference_id, tag_source_id=source.tag_source_id,
                              topic='ATP:0000009', entity_type='ATP:0000005', entity='WB:WBGene00003001',
                              entity_id_validation='alliance', species='NCBITaxon:6239', negated=False,
                              data_novelty='ATP:0000335', data_context='ATP:0000325')
    db.add(tag)
    db.commit()
    return reference, source, tag


def test_inserts_and_updates_fill_the_mirrors(db):  # noqa: F811
    reference, source, tag = _tag(db)
    assert _mirrors(db, 'topic_entity_tag', tag.topic_entity_tag_id) == {
        'topic_curie': 'ATP:0000009', 'entity_type_curie': 'ATP:0000005', 'species_curie': 'NCBITaxon:6239',
        'display_tag_curie': None, 'data_novelty_curie': 'ATP:0000335', 'data_context_curie': 'ATP:0000325'}
    assert _mirrors(db, 'tag_source', source.tag_source_id) == {'source_evidence_assertion_curie': 'ECO:0006155'}

    tag.topic = 'ATP:0000079'
    db.commit()
    assert _mirrors(db, 'topic_entity_tag', tag.topic_entity_tag_id)['topic_curie'] == 'ATP:0000079'

    # raw SQL on the id column (as set_retraction_status does) fills the mirror too
    db.execute(text("UPDATE reference SET retraction_status_id = (SELECT id FROM ontologyterm "
                    "WHERE curie = 'ATP:0000346') WHERE id = :id"), {"id": reference.reference_id})
    db.commit()
    assert _mirrors(db, 'reference', reference.reference_id) == {'retraction_status_curie': 'ATP:0000346'}


def test_a_mirror_cannot_be_written_directly(db):  # noqa: F811
    _reference, _source, tag = _tag(db)
    db.execute(text("UPDATE topic_entity_tag SET topic_curie = 'ATP:wrong' WHERE id = :id"),
               {"id": tag.topic_entity_tag_id})
    db.commit()
    assert _mirrors(db, 'topic_entity_tag', tag.topic_entity_tag_id)['topic_curie'] == 'ATP:0000009'


def test_installing_again_changes_nothing(db):  # noqa: F811
    # create_all_triggers runs on every start: once the columns and triggers exist it is a no-op
    add_curie_mirror_triggers(db)
    add_curie_mirror_triggers(db)
    triggers = db.execute(text("SELECT count(*) FROM pg_trigger WHERE tgname LIKE '%_curie_mirror'")).scalar()
    assert triggers == len(CURIE_MIRRORS)


def test_ksql_reads_the_mirrors_not_the_dropped_columns():
    for table, pairs in CURIE_MIRRORS.items():
        for _id_column, curie in pairs:
            assert re.search(rf"\b{curie} string\b", KSQL), f"ksql does not declare {table}.{curie}"
    tet_source = KSQL.split("CREATE TABLE topic_entity_tag(", 1)[1].split(") WITH", 1)[0]
    for dropped in ('\\"topic\\" string', 'entity_type string', 'species string', 'display_tag string',
                    'data_novelty string', 'data_context string'):
        assert dropped not in tet_source, dropped
