from ...api.fixtures import auth_headers  # noqa
from ...api.test_mod import test_mod  # noqa
from ...api.test_reference import test_reference  # noqa
from ...fixtures import db  # noqa
from agr_literature_service.api.models import ModModel, ReferenceModel, TagSourceModel, TopicEntityTagModel
from agr_literature_service.lit_processing.data_check.report_obsolete_entities import get_unique_entity_list


def _add_entity_tag(db, reference_id, data_provider, owner_mod_id, entity):  # noqa
    source = TagSourceModel(source_evidence_assertion="ECO:0006156", source_method=f"{data_provider} test pipeline",
                            description="test source", data_provider=data_provider,
                            secondary_data_provider_id=owner_mod_id)
    db.add(source)
    db.flush()
    db.add(TopicEntityTagModel(reference_id=reference_id, topic="ATP:0000005", entity_type="ATP:0000005",
                               entity=entity, entity_id_validation="alliance", species="NCBITaxon:7227",
                               tag_source_id=source.tag_source_id, negated=False,
                               data_novelty="ATP:0000335", data_context="ATP:0000325"))
    db.commit()


class TestGetUniqueEntityList:

    def test_entities_are_scoped_by_secondary_data_provider(self, db, test_mod, test_reference):  # noqa
        # The report checks each MOD's entities. A third-party pipeline (SCRUM-6338)
        # sets data_provider to where the data came from (e.g. GEO) and
        # secondary_data_provider to the owning MOD, so a data_provider filter missed
        # those entities entirely.
        reference_id = db.query(ReferenceModel.reference_id).filter_by(curie=test_reference.new_ref_curie).scalar()
        other_mod = ModModel(abbreviation="0016_BtDB", short_name="BtDB", full_name="Second test db")
        db.add(other_mod)
        db.commit()
        _add_entity_tag(db, reference_id, "GEO", test_mod.new_mod_id, "TEST:geo_owned_gene")
        _add_entity_tag(db, reference_id, test_mod.new_mod_abbreviation, other_mod.mod_id, "TEST:other_owned_gene")

        entities = {row[0] for row in get_unique_entity_list(db, test_mod.new_mod_abbreviation)["ATP:0000005"]}

        assert "TEST:geo_owned_gene" in entities
        # data_provider names this MOD, but another MOD owns the tag
        assert "TEST:other_owned_gene" not in entities
