import logging
from os import path
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_download_pmc_files \
    import upload_suppl_file_to_s3


# load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_bucket = 'agr-literature'
dataDir = path.dirname(path.abspath(__file__)) + "/data_for_pubmed_processing/"


def upload_pdf_files():  # pragma: no cover

    """
    980419	PMID:25429618	AGRKB:101000000980419	771a037417b69ebfe077acd5f9575edb     7/7/1/a
    980695	PMID:37867934	AGRKB:101000000980695	827cfe721c47abda8ac886fc22befed4     8/2/7/c
    980407	PMID:25309320	AGRKB:101000000980407	aae9fedd2a3616d2e5c303cc589eaca3     a/a/e/9
    980401	PMID:29163195	AGRKB:101000000980401	d5bbaaccf2f953044a009e08ef1b4d5f     d/5/b/b
    980327	PMID:33769615	AGRKB:101000000980327	e1e20d1bcb657945885003aa2f34dd74     e/1/e/2
    980376	PMID:33384586	AGRKB:101000000980376	e60fe1b06f90a74a27241bdede662f98     e/6/0/f
    """
    for md5sum in ['771a037417b69ebfe077acd5f9575edb', '827cfe721c47abda8ac886fc22befed4',
                   'aae9fedd2a3616d2e5c303cc589eaca3', 'd5bbaaccf2f953044a009e08ef1b4d5f',
                   'e1e20d1bcb657945885003aa2f34dd74', 'e60fe1b06f90a74a27241bdede662f98']:
        logger.info(f"Uploading PDF with md5sum: {md5sum} to s3")
        pdf_file = dataDir + md5sum + ".gz"
        upload_suppl_file_to_s3(pdf_file, md5sum)


if __name__ == "__main__":

    upload_pdf_files()
