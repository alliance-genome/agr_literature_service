# Canonical URLs for resources, used to populate the Location header on
# 201 Created responses (SCRUM-5716 strict-REST migration).
#
# Helpers are added as endpoints adopt the new convention. The path templates
# here mirror the route definitions in agr_literature_service/api/routers/.


def author_url(author_id: int) -> str:
    return f"/author/{author_id}"


def curation_status_url(curation_status_id: int) -> str:
    return f"/curation_status/{curation_status_id}"
