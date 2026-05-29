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


def reference_url(curie: str) -> str:
    return f"/reference/{curie}"


def resource_url(curie: str) -> str:
    return f"/resource/{curie}"


def image_permission_url(image_permission_id: int) -> str:
    return f"/image_permission/{image_permission_id}"


def resource_image_permission_url(resource_image_permission_id: int) -> str:
    return f"/image_permission/resource_link/{resource_image_permission_id}"


def referencefile_mod_url(referencefile_mod_id: int) -> str:
    return f"/reference/referencefile_mod/{referencefile_mod_id}"


def referencefile_url(referencefile_id: int) -> str:
    return f"/reference/referencefile/{referencefile_id}"


def person_url(curie: str) -> str:
    return f"/person/{curie}"
