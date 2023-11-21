import logging
from os import environ
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3


load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_bucket = 'agr-literature'
dataDir = 'data_for_pubmed_processing/'


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
        upload_pdf_file_to_s3(pdf_file, md5sum)


def upload_pdf_file_to_s3(gzip_file_with_path, md5sum):  # pragma: no cover

    if environ.get('ENV_STATE') is None or environ.get('ENV_STATE') == 'test':
        return

    s3_file_path = "/reference/documents/"

    storage = None
    if environ.get('ENV_STATE') == 'prod':
        s3_file_path = 'prod' + s3_file_path
        storage = 'GLACIER_IR'
    else:
        s3_file_path = 'develop' + s3_file_path
        storage = 'STANDARD'
    s3_file_path = s3_file_path + md5sum[0] + "/" + md5sum[1] + \
        "/" + md5sum[2] + "/" + md5sum[3] + "/"
    s3_file_location = s3_file_path + md5sum + ".gz"

    logger.info("Uploading " + gzip_file_with_path.split("/")[-1] + " to AGR s3: " + s3_file_location)

    status = upload_file_to_s3(gzip_file_with_path, s3_bucket, s3_file_location, storage)

    return status


if __name__ == "__main__":

    upload_pdf_files()
