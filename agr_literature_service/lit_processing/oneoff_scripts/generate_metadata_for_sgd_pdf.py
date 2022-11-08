import logging
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

infile = "data/pdf_md5sum.txt"
outfile = "data/pdf_metadata.txt"


def generate_metadata():

    db_session = create_postgres_session(False)

    pmid_to_reference_id = {}
    rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie_prefix = 'PMID'")
    rows = rs.fetchall()
    for x in rows:
        pmid = x[0].replace("PMID:", "")
        pmid_to_reference_id[pmid] = x[1]

    f = open(infile)
    fw = open(outfile, "w")

    for line in f:
        pieces = line.strip().split("\t")
        pmid = pieces[0]
        md5sum = pieces[1]
        if pmid not in pmid_to_reference_id:
            logger.info("PMID:" + pmid + " is not in the database.")
            continue
        reference_id = pmid_to_reference_id[pmid]
        fw.write(pmid + "\t" + str(reference_id) + "\t" + md5sum + "\n")
    f.close()
    fw.close()
    db_session.close()


if __name__ == "__main__":

    generate_metadata()
