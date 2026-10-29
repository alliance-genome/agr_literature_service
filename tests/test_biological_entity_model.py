"""SCRUM-6720 Phase 2: topic_entity_tag.entity_id follows the entity string (biological_entity_model)."""
from sqlalchemy import text

from agr_literature_service.api.models import (BiologicalEntityModel, ModModel, ReferenceModel, TagSourceModel,
                                               TopicEntityTagModel)

from .fixtures import db  # noqa: F401

GENE = 'WB:WBGene00003001'  # lin-12, seeded into the biologicalentity fixture


def _tag(db, entity, entity_type='ATP:0000005', species='NCBITaxon:6239'):  # noqa: F811
    mod = db.query(ModModel).first() or ModModel(abbreviation='WB', short_name='WB', full_name='WormBase')
    reference = ReferenceModel(curie=f'AGRKB:6720{abs(hash(entity)) % 10 ** 12}', category='research_article')
    db.add_all([mod, reference])
    db.flush()
    source = TagSourceModel(source_evidence_assertion='ECO:0006155', source_method='abc_literature_system',
                            description='curator', data_provider='WB', secondary_data_provider_id=mod.mod_id)
    db.add(source)
    db.flush()
    tag = TopicEntityTagModel(reference_id=reference.reference_id, tag_source_id=source.tag_source_id,
                              topic='ATP:0000005', entity_type=entity_type, entity=entity,
                              entity_id_validation='alliance', species=species, negated=False,
                              data_novelty='ATP:0000335', data_context='ATP:0000325')
    db.add(tag)
    db.commit()
    return tag


def _entity_pk(db, curie):  # noqa: F811
    return db.query(BiologicalEntityModel.id).filter(BiologicalEntityModel.primaryexternalid == curie).scalar()


def test_new_tag_links_its_entity(db):  # noqa: F811
    tag = _tag(db, GENE)
    assert tag.entity_id is not None
    assert tag.entity_id == _entity_pk(db, GENE)


def test_entity_a_team_does_not_hold_stays_a_string(db):  # noqa: F811
    # an SGD protein complex, a deleted gene or a species tag: no biologicalentity, no error
    tag = _tag(db, 'NCBITaxon:6239', entity_type='ATP:0000123')
    assert tag.entity == 'NCBITaxon:6239'
    assert tag.entity_id is None


def test_changing_the_entity_relinks_it_and_records_history(db):  # noqa: F811
    tag = _tag(db, GENE)
    tag.entity = 'WB:not-in-ateam'
    db.commit()
    assert tag.entity_id is None
    tag.entity = GENE
    db.commit()
    assert tag.entity_id == _entity_pk(db, GENE)
    history = db.execute(text("SELECT entity, entity_id, entity_id_mod FROM topic_entity_tag_version "
                              "WHERE id = :id ORDER BY transaction_id"), {"id": tag.topic_entity_tag_id}).fetchall()
    assert [tuple(row) for row in history] == [
        (GENE, _entity_pk(db, GENE), True),  # the insert sets it
        ('WB:not-in-ateam', None, True),
        (GENE, _entity_pk(db, GENE), True),
    ]
