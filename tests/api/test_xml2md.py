"""Tests for the XML-to-Markdown conversion endpoint."""
from unittest.mock import patch

from fastapi import status
from starlette.testclient import TestClient

from agr_literature_service.api import auth as auth_module
from agr_literature_service.api.main import app
from ..fixtures import db  # noqa

# Minimal valid TEI XML
MINIMAL_TEI = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader><fileDesc>
    <titleStmt><title level="a" type="main">Test Title</title></titleStmt>
    <publicationStmt><publisher/></publicationStmt>
    <sourceDesc><biblStruct><monogr>
      <imprint><date/></imprint>
    </monogr></biblStruct></sourceDesc>
  </fileDesc><profileDesc/></teiHeader>
  <text><body><div><head>Intro</head><p>Hello world.</p></div></body></text>
</TEI>
"""

# Minimal valid JATS XML
MINIMAL_JATS = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<article><front><article-meta>
  <title-group><article-title>JATS Title</article-title></title-group>
</article-meta></front>
<body><sec><title>Intro</title><p>Body text.</p></sec></body></article>
"""


def _bypass_auth():
    """Context manager to bypass authentication for all endpoints."""
    return (
        patch.object(auth_module, 'is_skip_all_auth_ip', return_value=True),
        patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False),
    )


class TestXml2MdConvert:
    """Tests for POST /xml2md/convert endpoint."""

    def test_convert_tei_auto_detect(self, db):  # noqa: F811
        """TEI XML auto-detected and converted to Markdown."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert",
                files={"file": ("test.xml", MINIMAL_TEI, "text/xml")},
            )
            assert resp.status_code == status.HTTP_200_OK
            assert resp.headers["content-type"] == "text/plain; charset=utf-8"
            assert "# Test Title" in resp.text
            assert "Hello world." in resp.text

    def test_convert_jats_auto_detect(self, db):  # noqa: F811
        """JATS XML auto-detected and converted to Markdown."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert",
                files={"file": ("test.xml", MINIMAL_JATS, "text/xml")},
            )
            assert resp.status_code == status.HTTP_200_OK
            assert "# JATS Title" in resp.text
            assert "Body text." in resp.text

    def test_convert_explicit_tei_format(self, db):  # noqa: F811
        """Explicit source_format=tei works."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert?source_format=tei",
                files={"file": ("test.xml", MINIMAL_TEI, "text/xml")},
            )
            assert resp.status_code == status.HTTP_200_OK
            assert "# Test Title" in resp.text

    def test_convert_explicit_jats_format(self, db):  # noqa: F811
        """Explicit source_format=jats works."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert?source_format=jats",
                files={"file": ("test.xml", MINIMAL_JATS, "text/xml")},
            )
            assert resp.status_code == status.HTTP_200_OK
            assert "# JATS Title" in resp.text

    def test_convert_html_output(self, db):  # noqa: F811
        """output_format=html returns rendered HTML page."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert?output_format=html",
                files={"file": ("test.xml", MINIMAL_TEI, "text/xml")},
            )
            assert resp.status_code == status.HTTP_200_OK
            assert "text/html" in resp.headers["content-type"]
            assert "<!DOCTYPE html>" in resp.text
            assert "Test Title" in resp.text

    def test_invalid_source_format(self, db):  # noqa: F811
        """Invalid source_format returns 400."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert?source_format=docx",
                files={"file": ("test.xml", MINIMAL_TEI, "text/xml")},
            )
            assert resp.status_code == status.HTTP_400_BAD_REQUEST
            assert "Invalid source_format" in resp.text

    def test_invalid_output_format(self, db):  # noqa: F811
        """Invalid output_format returns 400."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert?output_format=pdf",
                files={"file": ("test.xml", MINIMAL_TEI, "text/xml")},
            )
            assert resp.status_code == status.HTTP_400_BAD_REQUEST
            assert "Invalid output_format" in resp.text

    def test_empty_file(self, db):  # noqa: F811
        """Empty uploaded file returns 422."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert",
                files={"file": ("empty.xml", b"", "text/xml")},
            )
            assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "empty" in resp.text.lower()

    def test_unrecognized_xml_format(self, db):  # noqa: F811
        """XML that is neither TEI nor JATS returns 422."""
        p1, p2 = _bypass_auth()
        with p1, p2, TestClient(app) as client:
            resp = client.post(
                "/xml2md/convert",
                files={"file": ("bad.xml", b"<html/>", "text/xml")},
            )
            assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "Conversion failed" in resp.text

    def test_requires_authentication(self, db):  # noqa: F811
        """Unauthenticated request returns 401."""
        with patch.object(auth_module, 'is_skip_all_auth_ip',
                          return_value=False), \
             patch.object(auth_module, 'is_skip_read_auth_ip',
                          return_value=False):
            with TestClient(app) as client:
                resp = client.post(
                    "/xml2md/convert",
                    files={"file": ("test.xml", MINIMAL_TEI, "text/xml")},
                )
                assert resp.status_code == status.HTTP_401_UNAUTHORIZED
