from agr_literature_service.api.models.reference_mod_md5sum_model import ReferenceModMd5sumModel
from agr_literature_service.api.models import ReferenceModel, ModModel, CrossReferenceModel
from ..fixtures import db, populate_test_mod_reference_types # noqa
from sqlalchemy import and_


class TestReferenceModMd5sum:

    def test_reference_mod_md5sum(self, db): # noqa
        mod_data = {
            "abbreviation": "FB",
            "short_name": "FlyBase",
            "full_name": "FlyBase"
        }
        try:
            x = ModModel(**mod_data)
            db.add(x)
            print("Insert " + mod_data["abbreviation"] + " info into Mod table.")
        except Exception as e:
            print("An error occurred when inserting " + mod_data["abbreviation"] + " info into Mod table. " + str(e))

        mod_obj = db.query(ModModel).filter(ModModel.abbreviation == "FB").one_or_none()
        assert mod_obj is not None
        mod_id_FB = mod_obj.mod_id

        new_reference = {
            "title": "Bob",
            "category": "thesis",
            "abstract": "3",
            "curie": "AGR:AGR-Reference-0000808175"
        }
        try:
            ref_model = ReferenceModel(**new_reference)
            db.add(ref_model)
            print("insert reference wit title Bob")
        except Exception as e:
            print('Error: ' + str(type(e)))
        reference_obj = db.query(ReferenceModel).filter(ReferenceModel.title == "Bob").one_or_none()
        assert reference_obj is not None
        reference_id = reference_obj.reference_id
        new_md5sum = {
            "reference_id": reference_id,
            "mod_id": mod_id_FB,
            "md5sum": "2acc5bee41f60814a1d7eb7332445ebeTEST",
            "date_updated": "2021-11-07 14:06:00.686768"
        }
        new_crossref = {
            "date_created": "2021-11-07 14:06:00.686768",
            "reference_id": reference_id,
            "curie_prefix": "FB",
            "curie": "FB:FBrf000000001"
        }
        try:
            crossref_model = CrossReferenceModel(**new_crossref)
            db.add(crossref_model)
            md5_model = ReferenceModMd5sumModel(**new_md5sum)
            db.add(md5_model)
            print("insert data into reference_mod_md5sum")
        except Exception as e:
            print('Error: ' + str(type(e)))
        reference_mod_md5sum_obj = db.query(ReferenceModMd5sumModel).filter(and_(ReferenceModMd5sumModel.reference_id == reference_id,
                                                                            ReferenceModMd5sumModel.mod_id == mod_id_FB,
                                                                            ReferenceModMd5sumModel.md5sum == new_md5sum["md5sum"])).one_or_none()
        db.commit()
        assert reference_mod_md5sum_obj is not None
