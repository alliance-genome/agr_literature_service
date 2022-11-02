import logging
import sys
import math
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferencetypeModel, ModReferencetypeAssociationModel, ModModel
# from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
                    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def populate_mrt(db_session, mod_to_types_to_count):
    # labels_in = set()
    for mod_abbrev in mod_to_types_to_count.keys():
        mod = db_session.query(ModModel).filter(ModModel.abbreviation == mod_abbrev).one()
        # for label in mod_to_types_to_count[mod_abbrev].keys():
        #     if label not in labels_in:
        #         print(label)
        #         labels_in.add(label)
        #         rt_obj = ReferencetypeModel(label=label)
        #         db_session.add(rt_obj)
        display_order = 10
        for reference_type, value in sorted(mod_to_types_to_count[mod_abbrev].items(), key=lambda item: int(item[1]), reverse=True):
            logger.info(f"{mod_abbrev}\t{reference_type}\t{value}")
            rt_obj = db_session.query(ReferencetypeModel).filter(ReferencetypeModel.label == reference_type).one_or_none()
            if rt_obj is None:
                rt_obj = ReferencetypeModel(label=reference_type)
                db_session.add(rt_obj)
            mod_reference_type_obj = ModReferencetypeAssociationModel(mod=mod, referencetype=rt_obj,
                                                                      display_order=display_order)
            db_session.add(mod_reference_type_obj)
            display_order = math.ceil((display_order + 1) / 10) * 10
    db_session.commit()


def read_files():
    mod_list = ['FB', 'MGI', 'RGD', 'SGD', 'WB', 'XB', 'ZFIN']
    mod_to_types_to_count = {}
    for mod in mod_list:
        mod_to_types_to_count[mod] = {}
        with open('mrtInit/' + mod + '_mrt.txt', 'r') as f:
            lines = f.readlines()
            lines.pop(0)
            for line in lines:
                pieces = line.strip().split('\t')
                # mod_to_types_to_count[mod] = pieces[0]
                mod_to_types_to_count[mod][pieces[0]] = pieces[1]
    # print(mod_to_types_to_count)
    return mod_to_types_to_count


if __name__ == "__main__":
    db_session = create_postgres_session(False)
    # populate_test_mods()
    mod_to_types_to_count = read_files()
    populate_mrt(db_session, mod_to_types_to_count)
    db_session.close()
