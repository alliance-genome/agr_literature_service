from os import path

from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references


def load_references():

    json_file_path = path.dirname(path.abspath(__file__)) + "/sample_json/"

    post_references(json_path=json_file_path)
