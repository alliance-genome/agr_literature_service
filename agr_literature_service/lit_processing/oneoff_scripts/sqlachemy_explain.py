"""
Example of getting output of what a sqlalchemy query is doing.
Feel free to add examples.
"""
from sqlalchemy.dialects import postgresql

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel
from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel


def examine_query():
    mod_abbreviation = "WB"
    db_session = create_postgres_session(False)

    example_qs = []
    q = db_session.query(ReferenceModel
                         ).filter(ReferenceModel.prepublication_pipeline == True  # noqa
                                  ).join(ReferenceModel.mod_corpus_association
                                         ).join(ModCorpusAssociationModel.mod
                                                ).filter(ModModel.abbreviation == mod_abbreviation
                                                         ).join(CrossReferenceModel,
                                                                CrossReferenceModel.reference_id == ReferenceModel.reference_id
                                                                ).filter(CrossReferenceModel.curie_prefix == 'PMID'
                                                                         ).order_by(ReferenceModel.curie.desc
                                                                                    )
    example_qs.append(q)
    # add more example here as needed.

    for example_q in example_qs:
        sql = example_q.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
        sqlp = str(sql).replace('JOIN', '\n\tJOIN')
        sqlp = sqlp.replace('AND', '\n\tAND')
        sqlp = sqlp.replace('ORDER', '\n\tORDER')
        print(sqlp)
        for output_line in db_session.execute(f"EXPLAIN ANALYZE {sql}").fetchall():
            print(output_line)

    db_session.close()


if __name__ == "__main__":

    examine_query()
