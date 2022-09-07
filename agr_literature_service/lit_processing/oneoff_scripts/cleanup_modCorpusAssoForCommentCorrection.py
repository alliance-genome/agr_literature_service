from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.models import ModCorpusAssociationModel
import logging

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def cleanup_data():

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    db_session = create_postgres_session(False)

    # in rdsdev
    # select * from mod_corpus_association where reference_id in
    # (select reference_id_from from reference_comments_and_corrections);
    # 930

    # select * from mod_corpus_association where reference_id in
    # (select reference_id_to from reference_comments_and_corrections);
    # 6281

    # select distinct reference_id_to from reference_comments_and_corrections;
    # 6382

    # for reference_id_to
    # https://stage-literature-rest.alliancegenome.org/reference/AGR:AGR-Reference-0000869198
    # https://stage-literature-rest.alliancegenome.org/reference/AGR:AGR-Reference-0000000194
    # https://stage-literature-rest.alliancegenome.org/reference/AGR:AGR-Reference-0000000376

    # for reference_id_from
    # https://stage-literature-rest.alliancegenome.org/reference/AGR:AGR-Reference-0000000248

    log.info("Retrieving data from mod_corpus_association table...")

    reference_id_to_mca_rows = get_mod_corpus_association_data(db_connection)

    log.info("Retrieving data from reference_comments_and_corrections table...")

    rs = db_connection.execute("SELECT reference_id_from, reference_id_to from reference_comments_and_corrections")

    rows = rs.fetchall()

    log.info("Adding missing rows into mod_corpus_association table...")

    found = {}
    inserted = {}
    i = 0
    for x in rows:

        reference_id_from = x[0]
        reference_id_to = x[1]

        if (reference_id_from, reference_id_to) in found:
            continue
        found[(reference_id_from, reference_id_to)] = 1

        # print ("reference_id_to  =", reference_id_to, reference_id_to_mca_rows.get(reference_id_to))
        # print ("reference_id_from=", reference_id_from, reference_id_to_mca_rows.get(reference_id_from))
        if reference_id_to not in reference_id_to_mca_rows and reference_id_from not in reference_id_to_mca_rows:
            ## both 'to' and 'from' papers are not in mod_corpus_association table
            continue
        if reference_id_from not in reference_id_to_mca_rows:
            for row in reference_id_to_mca_rows[reference_id_to]:
                (mod_id, source, corpus) = row
                insert_mod_corpus_association(db_session, reference_id_from,
                                              mod_id, source, corpus, inserted,
                                              "FROM")
        elif reference_id_to not in reference_id_to_mca_rows:
            for row in reference_id_to_mca_rows[reference_id_from]:
                (mod_id, source, corpus) = row
                insert_mod_corpus_association(db_session, reference_id_to,
                                              mod_id, source, corpus, inserted,
                                              "TO")
        else:
            mod_id_to_row_from = {}
            for from_row in reference_id_to_mca_rows[reference_id_from]:
                (mod_id_from, source_from, corpus_from) = from_row
                mod_id_to_row_from[mod_id_from] = (source_from, corpus_from)
            mod_id_to_row_to = {}
            for to_row in reference_id_to_mca_rows[reference_id_to]:
                (mod_id_to, source_to, corpus_to) = to_row
                mod_id_to_row_to[mod_id_to] = (source_to, corpus_to)
                if mod_id_to not in mod_id_to_row_from:
                    ## the 'from' paper is not associated with the mod (mod_id_to)
                    ## that the 'to' (main) paper is associated with, so add this
                    ## association
                    insert_mod_corpus_association(db_session, reference_id_from,
                                                  mod_id_to, source_to, corpus_to,
                                                  inserted, "FROM")
                else:
                    (source_from, corpus_from) = mod_id_to_row_from[mod_id_to]
                    if corpus_from != corpus_to:
                        ## although they are associated with same mod, but they have
                        ## different 'corpus' so set the "from" paper's corpus to
                        ## it's main paper's corpus if main paper's corpus is True
                        if corpus_to:
                            update_mod_corpus_association(db_session, reference_id_from,
                                                          mod_id_from, corpus_to, "FROM")
                        else:
                            # from: https://stage-literature-rest.alliancegenome.org/reference/AGR:AGR-Reference-0000640987
                            # to: https://stage-literature-rest.alliancegenome.org/reference/AGR:AGR-Reference-0000397010
                            update_mod_corpus_association(db_session, reference_id_to,
                                                          mod_id_to, corpus_from, "TO")

            for mod_id_from in mod_id_to_row_from:
                if mod_id_from in mod_id_to_row_to:
                    continue
                (source_from, corpus_from) = mod_id_to_row_from[mod_id_from]
                ## the 'to' (main) paper is not associated with the mod (mod_id_from)
                ## that the 'from' paper is associated with so add this association
                insert_mod_corpus_association(db_session, reference_id_to, mod_id_from,
                                              source_from, corpus_from, inserted, "TO")

        i += 1
        if i % 250 == 0:
            db_session.commit()

    db_session.commit()
    db_session.close()
    db_connection.close()
    engine.dispose()


def update_mod_corpus_association(db_session, reference_id, mod_id, corpus_new, fromTo):

    try:
        x = db_session.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one_or_none()
        if x is None:
            return
        x.corpus = corpus_new
        db_session.add(x)

        log.info(fromTo + " paper: the 'corpus' has been updated to " + str(corpus_new) + " for reference_id = " + str(reference_id) + " and mod_id = " + str(mod_id))
    except Exception as e:
        log.info("An error occurred when updating corpus for for reference_id = " + str(reference_id) + " and mod_id = " + str(mod_id) + " error: " + str(e))


def insert_mod_corpus_association(db_session, reference_id, mod_id, source, corpus, inserted, fromTo):

    if (reference_id, mod_id) in inserted:
        return

    try:
        row = {'reference_id': reference_id,
               "mod_id": mod_id,
               "mod_corpus_sort_source": source,
               "corpus": corpus}

        mca_obj = ModCorpusAssociationModel(**row)
        db_session.add(mca_obj)

        inserted[(reference_id, mod_id)] = 1

        log.info(fromTo + " paper: " + "A new row has been added into mod_corpus_association for reference_id = " + str(reference_id) + ", mod_id = " + str(mod_id) + ", mod_corpus_sort_source = " + source + " and corpus = " + str(corpus))
    except Exception as e:
        log.info("An error occurred when adding a new row for reference_id = " + str(reference_id) + ", mod_id = " + str(mod_id) + ", mod_corpus_sort_source = " + source + " and corpus = " + str(corpus) + " error=" + str(e))


def get_mod_corpus_association_data(db_connection):

    reference_id_to_mca_rows = {}

    rs = db_connection.execute("SELECT reference_id, mod_id, mod_corpus_sort_source, corpus from mod_corpus_association")

    rows = rs.fetchall()

    # i = 0
    for x in rows:
        reference_id = x[0]
        mod_id = x[1]
        source = x[2]
        corpus = x[3]
        rows = []
        if reference_id in reference_id_to_mca_rows:
            rows = reference_id_to_mca_rows[reference_id]
        rows.append((mod_id, source, corpus))
        reference_id_to_mca_rows[reference_id] = rows
        # i += 1
        # print (i, (mod_id, source, corpus), reference_id_to_mca_rows[reference_id])

    return reference_id_to_mca_rows


if __name__ == "__main__":

    cleanup_data()
