import logging
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModReferenceTypeAssociationModel, ModReferenceTypeModel, \
    ReferenceModReferenceTypeAssociationModel

logger = logging.getLogger(__name__)


def populate_mrt_from_old_table():
    db_session = create_postgres_session(False)
    all_mrts = db_session.query(ModReferenceTypeAssociationModel).all()
    modabbr_mrtlabel__mrtid = {(mrt.mod.abbreviation, mrt.referencetype.label): mrt.mod_referencetype_id for mrt in
                               all_mrts}
    old_rmrts = db_session.query(ModReferenceTypeModel).all()
    for old_rmrt in old_rmrts:
        db_session.add(ReferenceModReferenceTypeAssociationModel(
            reference_id=old_rmrt.reference_id,
            mod_referencetype_id=modabbr_mrtlabel__mrtid[(old_rmrt.source, old_rmrt.reference_type)]))
    db_session.commit()
    db_session.close()


if __name__ == "__main__":
    populate_mrt_from_old_table()
