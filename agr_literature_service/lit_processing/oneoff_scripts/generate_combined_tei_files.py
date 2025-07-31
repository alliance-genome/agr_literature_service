import logging
import os
import warnings
import requests
from lxml import etree
from sqlalchemy import text, bindparam
from copy import deepcopy
import tempfile
import camelot

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.referencefile_crud import download_file
from agr_literature_service.api.routers.okta_utils import OktaAccess

NS = "http://www.tei-c.org/ns/1.0"
# Suppress Camelot 'no tables' UserWarnings
warnings.filterwarnings("ignore", category=UserWarning, module="camelot")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

mod_curies_file = "data/tfp_strain.tsv"
output_dir = "combined_tei/"


reference_curies = [
    'AGRKB:101000000973629',
    'AGRKB:101000000991149',
    'AGRKB:101000000947459',
    'AGRKB:101000000957422',
    'AGRKB:101000000868286',
    'AGRKB:101000000640929',
    'AGRKB:101000000954438',
    'AGRKB:101000000980851',
    'AGRKB:101000000961832',
    'AGRKB:101000000868965',
    'AGRKB:101000000640190',
    'AGRKB:101000000872562',
    'AGRKB:101000000638714',
    'AGRKB:101000000967099',
    'AGRKB:101000000970829',
    'AGRKB:101000000641039',
    'AGRKB:101000000985421',
    'AGRKB:101000000639549',
    'AGRKB:101000000638182',
    'AGRKB:101000000641137'
]


def get_curie_to_reffile_id_mapping2(db):
    # 1) Load CURIEs
    with open(mod_curies_file, "r") as f:
        mod_curies = [line.split("\t", 1)[0] for line in f]

    # 2) Raw SQL via text(), with expanding bind‑param for large lists
    sql = text("""
        SELECT
          r.curie AS curie,
          rf.referencefile_id AS referencefile_id,
          rf.file_class
        FROM reference r
        JOIN cross_reference cr
          ON r.reference_id = cr.reference_id
        JOIN referencefile rf
          ON cr.reference_id = rf.reference_id
        WHERE cr.curie IN :mod_curies
          AND rf.file_extension = 'pdf'
          AND rf.pdf_type = 'pdf'
          AND rf.file_publication_status = 'final'
        ORDER BY
          r.curie,
          CASE rf.file_class
            WHEN 'main' THEN 0
            WHEN 'supplement' THEN 1
            ELSE 2
        END
    """).bindparams(bindparam("mod_curies", expanding=True))

    rows = db.execute(sql, {"mod_curies": mod_curies}).all()

    # 3) Group into { curie: [ref_file_id, ...] }
    mapping = {}
    for curie, rf_id, file_class in rows:
        mapping.setdefault(curie, []).append((rf_id, file_class))
    return mapping


def get_curie_to_reffile_id_mapping(db):

    sql = text("""
        SELECT
          r.curie AS curie,
          rf.referencefile_id AS referencefile_id,
          rf.file_class
        FROM reference r
        JOIN referencefile rf
          ON r.reference_id = rf.reference_id
        WHERE r.curie IN :ref_curies
          AND rf.file_extension = 'pdf'
          AND rf.pdf_type = 'pdf'
          AND rf.file_publication_status = 'final'
        ORDER BY
          r.curie,
          CASE rf.file_class
            WHEN 'main'         THEN 0
            WHEN 'supplement' THEN 1
            ELSE 2
        END
    """).bindparams(bindparam("ref_curies", expanding=True))
    rows = db.execute(sql, {"ref_curies": reference_curies}).all()
    mapping = {}
    for curie, rf_id, file_class in rows:
        mapping.setdefault(curie, []).append((rf_id, file_class))
    return mapping


def convert_pdf_with_grobid(file_content: bytes) -> requests.Response:
    """Send PDF bytes to GROBID and return the HTTP response."""
    # "https://grobid.alliancegenome.org/api/processFulltextDocument"
    # http://cervino.caltech.edu:8070/
    url = os.environ.get(
        "PDF2TEI_API_URL",
        "http://cervino.caltech.edu:8070/api/processFulltextDocument"
    )
    return requests.post(url, files={"input": ("file", file_content)})


def combine_tei_documents(tei_roots):
    """
    Given a list of parsed TEI roots, produce a single combined TEI tree.
    We merge all <body> children under one master <body>, stripping xml:id and id attributes
    to avoid duplicates and ensure valid TEI.
    Also include document-level <head>, <title>, and <abstract> from the first TEI - from main PDF.
    """
    nsmap = {None: NS}
    master = etree.Element(f"{{{NS}}}TEI", nsmap=nsmap)
    tei_text = etree.SubElement(master, f"{{{NS}}}text")
    # Add front section for title and abstract
    tei_front = etree.SubElement(tei_text, f"{{{NS}}}front")
    # extract title and abstract from the first TEI root
    first_root = tei_roots[0]
    head_el = first_root.find(f".//{{{NS}}}head")
    if head_el is not None:
        tei_front.append(deepcopy(head_el))
    title_el = first_root.find(f".//{{{NS}}}title")
    if title_el is not None:
        tei_front.append(deepcopy(title_el))
    abstract_el = first_root.find(f".//{{{NS}}}abstract")
    if abstract_el is not None:
        tei_front.append(deepcopy(abstract_el))
    # create combined body
    tei_body = etree.SubElement(tei_text, f"{{{NS}}}body")

    for root in tei_roots:
        # Remove any xml:id or generic id attributes in this root subtree
        for el in root.iter():
            xml_id_attr = "{http://www.w3.org/XML/1998/namespace}id"
            if xml_id_attr in el.attrib:
                del el.attrib[xml_id_attr]
            if "id" in el.attrib:
                del el.attrib["id"]

        # Append all children of <body> into the combined TEI body
        body = root.find(f".//{{{NS}}}body")
        if body is not None:
            for child in body:
                tei_body.append(child)

    return master


def main():
    os.makedirs(output_dir, exist_ok=True)
    db = create_postgres_session(False)
    errors = []

    curie_to_reffile_id_mapping = get_curie_to_reffile_id_mapping(db)

    for reference_curie, ref_ids_with_file_class in curie_to_reffile_id_mapping.items():
        tei_roots = []
        table_elems_accum = []
        for (ref_file_id, _file_class) in ref_ids_with_file_class:
            try:
                # Download PDF bytes
                file_content = download_file(
                    db=db,
                    referencefile_id=ref_file_id,
                    mod_access=OktaAccess.ALL_ACCESS,
                    use_in_api=False
                )

                # 1) Always convert via GROBID
                resp = convert_pdf_with_grobid(file_content)
                resp.raise_for_status()
                root = etree.fromstring(resp.content)
                tei_roots.append(root)
                logger.info(f"GROBID converted PDF {ref_file_id} for {reference_curie}")

                # 2) Always extract tables via Camelot
                tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
                tmp.write(file_content)
                tmp.flush()
                tmp_path = tmp.name
                tmp.close()
                try:
                    # lattice parser - bordered (lattice) tables
                    try:
                        lattice_tables = camelot.read_pdf(tmp_path, pages='all', flavor='lattice')
                    except Exception as e:
                        logger.warning(f"Lattice parse failed for {reference_curie}/{ref_file_id}: {e}")
                        lattice_tables = []
                    # stream parser - whitespace‐aligned (stream) tables
                    try:
                        stream_tables = camelot.read_pdf(tmp_path, pages='all', flavor='stream')
                    except Exception as e:
                        logger.warning(f"Stream parse failed for {reference_curie}/{ref_file_id}: {e}")
                        stream_tables = []
                    # combine unique
                    tables = list(lattice_tables) + [tbl for tbl in stream_tables if tbl not in lattice_tables]
                    # convert to TEI <table>
                    for table in tables:
                        df = table.df
                        tbl_elem = etree.Element(f"{{{NS}}}table", nsmap={None: NS})
                        for _, row in df.iterrows():
                            tr = etree.SubElement(tbl_elem, f"{{{NS}}}row")
                            for cell in row:
                                td = etree.SubElement(tr, f"{{{NS}}}cell")
                                td.text = str(cell)
                        table_elems_accum.append(tbl_elem)
                finally:
                    os.unlink(tmp_path)

            except Exception as e:
                logger.error(f"Error processing PDF {ref_file_id} for {reference_curie}: {e}")
                errors.append((reference_curie, ref_file_id, str(e)))

        if not tei_roots:
            logger.warning(f"No TEI generated for {reference_curie}; skipping write")
            continue

        master_tei = combine_tei_documents(tei_roots)
        # append extracted tables from supplementary PDFs
        body = master_tei.find(f".//{{{NS}}}body")
        for tbl in table_elems_accum:
            body.append(tbl)
        combined_bytes = etree.tostring(master_tei, xml_declaration=True, encoding="utf-8")

        out_path = os.path.join(output_dir, f"{reference_curie}.combined.tei")
        with open(out_path, "wb") as out_f:
            out_f.write(combined_bytes)
        logger.info(f"Wrote combined TEI to {out_path}")

    db.close()

    if errors:
        logger.warning(f"Completed with {len(errors)} errors; see logs for details")


if __name__ == "__main__":
    main()
