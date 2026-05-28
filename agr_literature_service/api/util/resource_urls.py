# Canonical URLs for resources, used to populate the Location header on
# 201 Created responses (SCRUM-5716 strict-REST migration).
#
# Helpers are added as endpoints adopt the new convention. The path templates
# here mirror the route definitions in agr_literature_service/api/routers/.


def author_url(author_id: int) -> str:
    return f"/author/{author_id}"


def curation_status_url(curation_status_id: int) -> str:
    return f"/curation_status/{curation_status_id}"


def cross_reference_url(cross_reference_id: int) -> str:
    return f"/cross_reference/{cross_reference_id}"


def mod_url(abbreviation: str) -> str:
    # /mod GET-by-id is by abbreviation, not int PK; use it for the canonical URL.
    return f"/mod/{abbreviation}"


def editor_url(editor_id: int) -> str:
    return f"/editor/{editor_id}"


def mod_reference_type_url(mod_reference_type_id: int) -> str:
    return f"/reference/mod_reference_type/{mod_reference_type_id}"


def mesh_detail_url(mesh_detail_id: int) -> str:
    return f"/reference/mesh_detail/{mesh_detail_id}"


def mod_corpus_association_url(mod_corpus_association_id: int) -> str:
    return f"/reference/mod_corpus_association/{mod_corpus_association_id}"
