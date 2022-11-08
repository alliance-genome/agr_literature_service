import logging
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

infile = "data/PMC_OA_files_uploaded.txt"
outfile = "data/PMC_OA_metadata.txt"


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
        # 35857496	PMC9278858	sciadv.abm9875-f5.jpg	17ef0e061fcdc9bd1f4338809f738d72
        pieces = line.strip().split("\t")
        pmid = pieces[0]
        if pmid not in pmid_to_reference_id:
            logger.info("PMID:" + pmid + " is not in the database.")
            continue
        pmcid = pieces[1]
        file_name = pieces[2]
        md5sum = pieces[3]
        reference_id = pmid_to_reference_id[pmid]
        fw.write(pmid + "\t" + pmcid + "\t" + str(reference_id) + "\t" + md5sum + "\t" + file_name + "\n")
    f.close()
    fw.close()
    db_session.close()


if __name__ == "__main__":

    generate_metadata()
