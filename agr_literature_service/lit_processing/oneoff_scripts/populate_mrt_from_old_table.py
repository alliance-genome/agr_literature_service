import datetime
import itertools
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
    batch_size = 500
    total_start_time = datetime.datetime.now()
    for iteration_num in itertools.count(0):
        start_time = datetime.datetime.now()
        old_rmrts = db_session.query(ModReferenceTypeModel).order_by(
            ModReferenceTypeModel.mod_reference_type_id).offset(iteration_num * batch_size).limit(batch_size).all()
        if not old_rmrts:
            break
        for old_rmrt in old_rmrts:
            db_session.add(ReferenceModReferenceTypeAssociationModel(
               reference_id=old_rmrt.reference_id,
               mod_referencetype_id=modabbr_mrtlabel__mrtid[(old_rmrt.source, old_rmrt.reference_type)]))
        db_session.commit()
        end_time = datetime.datetime.now()
        print("batch processed in", end_time - start_time, "seconds")
        print("average processing time per batch so far", (end_time - total_start_time) / (iteration_num + 1), "seconds")
    db_session.close()
    total_end_time = datetime.datetime.now()
    print("total processing time", total_end_time - total_start_time, "seconds")


if __name__ == "__main__":
    populate_mrt_from_old_table()
