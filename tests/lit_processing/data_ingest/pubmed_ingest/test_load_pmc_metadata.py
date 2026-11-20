from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.load_pmc_metadata import \
    build_article_pdf_roots, determine_file_class, find_article_pdf_root, resolve_displayname_conflict

# the supplementary-material of a Cell Press author manuscript (PMC5823974)
CELL_PRESS_XML = """<article xmlns:xlink="http://www.w3.org/1999/xlink"><body>
<supplementary-material content-type="local-data" id="mmc1">
  <caption><p>Document S1. Figures S1&#8211;S5 and Tables S1&#8211;S5</p></caption>
  <media xlink:href="mmc1.pdf"/>
</supplementary-material>
<supplementary-material content-type="local-data" id="mmc2">
  <caption><p>Document S2. Article plus Supplemental Information</p></caption>
  <media xlink:href="mmc2.pdf"/>
</supplementary-material>
</body></article>"""


def _package(tmp_path, pmid, pmcid, files):  # noqa
    """Unpack a package as pubmed_download_pmc_files does and write its listing."""
    package = tmp_path / "pkg" / pmid / pmcid
    package.mkdir(parents=True)
    listing = tmp_path / "listing.txt"
    with listing.open("a") as f:
        for name, content in files.items():
            (package / name).write_text(content)
            f.write(f"{pmid}\t{pmcid}\t{name}\tmd5-{name}\t{len(content)}\n")
    return str(listing), str(tmp_path / "pkg")


class TestLoadPmcMetadata:

    def test_resolve_displayname_conflict(self):
        ref_file_uniq_filename_set = {(1, "file1.txt"), (1, "file2.txt"), (1, "file1_1.txt")}
        file_name_with_suffix = "file1.txt"
        reference_id = 1

        new_file_name = resolve_displayname_conflict(ref_file_uniq_filename_set,
                                                     file_name_with_suffix,
                                                     reference_id)
        assert new_file_name == 'file1_2.txt'


class TestArticlePlusSupplementPdf:

    def test_finds_the_article_plus_supplement_pdf(self):
        assert find_article_pdf_root(CELL_PRESS_XML) == "mmc2"
        lower_case = CELL_PRESS_XML.replace("Supplemental Information", "supplemental information")
        assert find_article_pdf_root(lower_case) == "mmc2"

    def test_no_article_pdf_among_plain_supplements(self):
        plain = CELL_PRESS_XML.replace("Article plus Supplemental Information", "Data S1")
        assert find_article_pdf_root(plain) is None
        assert find_article_pdf_root("<article/>") is None

    def test_article_pdf_becomes_main_and_xml_nxml(self, tmp_path):
        listing, package_dir = _package(tmp_path, "29452635", "PMC5823974", {
            "PMC5823974.1.xml": CELL_PRESS_XML, "mmc1.pdf": "s1", "mmc2.pdf": "s2"})
        article_pdf_roots = build_article_pdf_roots(listing, package_dir)
        assert article_pdf_roots == {"PMC5823974": "mmc2"}

        def file_class(name, ext):
            return determine_file_class(name, ext, "PMC5823974", {"PMC5823974": "pmc5823974.1"},
                                        {"PMC5823974": {"mmc1", "mmc2"}},
                                        pmcid_to_article_pdf_root=article_pdf_roots)

        assert file_class("mmc2", "pdf") == "main"
        assert file_class("mmc1", "pdf") == "supplement"
        assert file_class("PMC5823974.1", "xml") == "nXML"

    def test_pdf_named_like_the_xml_stays_the_main_pdf(self, tmp_path):
        # with a real article PDF, an "Article plus" supplement is left a supplement
        listing, package_dir = _package(tmp_path, "1", "PMC1", {
            "PMC1.1.xml": CELL_PRESS_XML, "PMC1.1.pdf": "article", "mmc2.pdf": "s2"})
        assert build_article_pdf_roots(listing, package_dir) == {}

    def test_missing_article_pdf_file_is_ignored(self, tmp_path):
        # the caption names a PDF the package does not ship
        listing, package_dir = _package(tmp_path, "2", "PMC2", {
            "PMC2.1.xml": CELL_PRESS_XML, "mmc1.pdf": "s1"})
        assert build_article_pdf_roots(listing, package_dir) == {}

    def test_without_article_pdf_xml_stays_supplement(self):
        assert determine_file_class("PMC3.1", "xml", "PMC3", {"PMC3": "pmc3.1"},
                                    {"PMC3": {"mmc1"}}) == "supplement"
