import logging
from typing import List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from agr_literature_service.api.models import (
    ReferenceModel, WorkflowTagModel, CrossReferenceModel,
    ModCorpusAssociationModel, ModModel, ResourceDescriptorModel,
    ReferencefileModAssociationModel
)
from agr_literature_service.api.schemas import (
    ReferenceSchemaNeedReviewShow, CrossReferenceSchemaShow,
    ReferencefileSchemaRelated, ReferencefileModSchemaShow,
    ModCorpusSortSourceType
)
from agr_literature_service.api.crud.reference_crud import get_past_to_present_date_range
from agr_literature_service.api.crud import search_crud

logger = logging.getLogger(__name__)


def convert_xref_curie_to_url(curie, resource_descriptor_default_urls):
    db_prefix, local_id = curie.split(":", 1)
    if db_prefix in resource_descriptor_default_urls:
        return resource_descriptor_default_urls[db_prefix].replace("[%s]", local_id)
    return None


def get_need_review_sort_sources(mod_abbreviation: str, db: Session) -> List[str]:
    """
    Get distinct mod_corpus_sort_source values for needs_review papers.

    Args:
        mod_abbreviation: The MOD abbreviation (e.g., 'WB', 'SGD')
        db: Database session

    Returns:
        List of sort source values that exist for this MOD's needs_review papers
    """
    result = db.query(
        ModCorpusAssociationModel.mod_corpus_sort_source
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModCorpusAssociationModel.corpus == None,  # noqa - needs review papers only
        ModModel.abbreviation == mod_abbreviation
    ).distinct().all()

    # Convert enum values to strings, filter out None, and sort
    sources = [row[0].value for row in result if row[0] is not None]
    return sorted(sources)


def _search_es_for_curies(
    mod_abbreviation: str,
    search_query: str,
    max_results: int = 500
) -> List[str]:
    """
    Use Elasticsearch to find matching reference curies for needs_review papers.

    Args:
        mod_abbreviation: The MOD abbreviation (e.g., 'WB', 'SGD')
        search_query: Keyword to search in title, abstract, journal, author
        max_results: Maximum number of curies to return

    Returns:
        List of curie strings matching the search
    """
    try:
        # Use existing search infrastructure with mods_needs_review filter
        result = search_crud.search_references(
            query=search_query,
            facets_values={"mods_needs_review.keyword": [mod_abbreviation]},
            size_result_count=max_results,
            page=1,
            return_facets_only=False,
            query_fields="All",
            partial_match=True
        )

        # Extract curies from search results
        curies = [hit.get("curie") for hit in result.get("hits", []) if hit.get("curie")]
        return curies
    except Exception as e:
        logger.error(f"ES search failed: {e}")
        return []


def _build_need_review_base_query(
    mod_abbreviation: str,
    db: Session,
    sort_source: Optional[str] = None
):
    """
    Build the base query for needs_review references (without text search).

    Returns the query object for reuse in count and results.
    """
    # Base query
    references_query = db.query(
        ReferenceModel
    ).join(
        ReferenceModel.mod_corpus_association
    ).filter(
        ModCorpusAssociationModel.corpus == None  # noqa
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModModel.abbreviation == mod_abbreviation
    )

    # Filter by mod_corpus_sort_source
    if sort_source:
        try:
            source_enum = ModCorpusSortSourceType(sort_source)
            references_query = references_query.filter(
                ModCorpusAssociationModel.mod_corpus_sort_source == source_enum
            )
        except ValueError:
            logger.warning(f"Invalid sort_source value: {sort_source}")

    return references_query


def show_need_review(
    mod_abbreviation: str,
    count: Optional[int],
    db: Session,
    search_query: Optional[str] = None,
    sort_source: Optional[str] = None,
    sort_by: str = "curie",
    sort_order: str = "desc"
):
    """
    Get references needing review with optional search, filter, and sort.

    Uses Elasticsearch for text search (fast), then fetches full details from DB.

    Args:
        mod_abbreviation: The MOD abbreviation (e.g., 'WB', 'SGD')
        count: Maximum number of results to return (default 100)
        db: Database session
        search_query: Optional keyword to search in title, abstract, journal, author
        sort_source: Optional mod_corpus_sort_source value to filter by
        sort_by: Field to sort by ('curie' or 'date_published')
        sort_order: Sort order ('asc' or 'desc')

    Returns:
        Dict with total_count and list of references matching the criteria
    """
    # Default limit
    if count is None:
        count = 100

    # Build base query (without text search)
    base_query = _build_need_review_base_query(mod_abbreviation, db, sort_source)

    # If there's a search query, use ES to get matching curies (fast text search)
    es_curies = None
    if search_query and search_query.strip():
        es_curies = _search_es_for_curies(mod_abbreviation, search_query, max_results=500)
        if not es_curies:
            # No ES matches - return empty result
            return {"total_count": 0, "references": []}
        # Filter DB query by ES-matched curies
        base_query = base_query.filter(ReferenceModel.curie.in_(es_curies))

    # Get total count
    total_count = base_query.distinct(ReferenceModel.reference_id).count()

    # Build full query for results with copyright license join
    references_query = base_query.outerjoin(ReferenceModel.copyright_license)

    # Apply sorting
    if sort_by == "date_published":
        order_col = ReferenceModel.date_published
    else:
        order_col = ReferenceModel.curie

    if sort_order == "asc":
        references_query = references_query.order_by(order_col.asc().nulls_last())
    else:
        references_query = references_query.order_by(order_col.desc().nulls_last())

    # Apply limit
    references_query = references_query.limit(count)

    references = references_query.all()
    results = show_sort_result(references, mod_abbreviation, db)

    return {
        "total_count": total_count,
        "references": results
    }


def show_sort_result(references, mod_abbreviation, db):
    resource_descriptor_default_urls = db.query(ResourceDescriptorModel).all()
    resource_descriptor_default_urls_dict = {
        resource_descriptor_default_url.db_prefix: resource_descriptor_default_url.default_url
        for resource_descriptor_default_url in resource_descriptor_default_urls}

    mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db.query(ModModel).all()])

    return [
        ReferenceSchemaNeedReviewShow(
            curie=reference.curie,
            title=reference.title,
            abstract=reference.abstract,
            category=reference.category,
            date_published=reference.date_published,
            copyright_license_name=reference.copyright_license.name if reference.copyright_license else "",
            copyright_license_url=reference.copyright_license.url if reference.copyright_license else "",
            copyright_license_description=reference.copyright_license.description if reference.copyright_license else "",
            copyright_license_open_access=reference.copyright_license.open_access if reference.copyright_license else "",
            cross_references=[CrossReferenceSchemaShow(
                cross_reference_id=xref.cross_reference_id, curie=xref.curie, curie_prefix=xref.curie_prefix,
                url=convert_xref_curie_to_url(xref.curie, resource_descriptor_default_urls_dict),
                is_obsolete=xref.is_obsolete, pages=xref.pages) for xref in reference.cross_reference],
            mod_corpus_association_corpus=[mca.corpus for mca in reference.mod_corpus_association if
                                           mca.mod.abbreviation == mod_abbreviation][0],
            mod_corpus_association_id=[mca.mod_corpus_association_id for mca in reference.mod_corpus_association if
                                       mca.mod.abbreviation == mod_abbreviation][0],
            prepublication_pipeline=reference.prepublication_pipeline,
            pubmed_publication_status=reference.pubmed_publication_status if reference.pubmed_publication_status else None,
            resource_title=reference.resource.title if reference.resource else "",
            referencefiles=[ReferencefileSchemaRelated(
                referencefile_id=rf.referencefile_id, display_name=rf.display_name,
                file_class=rf.file_class, file_publication_status=rf.file_publication_status,
                file_extension=rf.file_extension,
                md5sum=rf.md5sum, is_annotation=rf.is_annotation,
                referencefile_mods=get_referencefile_mod(rf.referencefile_id, db)) for rf in reference.referencefiles],
            authors=[{"order": author.order,
                      "author_id": author.author_id,
                      "name": author.name} for author in reference.author],
            workflow_tags=[{"reference_workflow_tag_id": wft.reference_workflow_tag_id,
                            "workflow_tag_id": wft.workflow_tag_id,
                            "mod_abbreviation": mod_id_to_mod.get(wft.mod_id, '')} for wft in reference.workflow_tag])
        for reference in references]


def show_prepublication_pipeline(mod_abbreviation, count, db: Session):
    query_ref_mod_curatability = db.query(
        WorkflowTagModel
    ).filter(
        WorkflowTagModel.workflow_tag_id.in_(['ATP:0000103', 'ATP:0000104', 'ATP:0000106'])
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModModel.abbreviation == mod_abbreviation
    )
    wfts = query_ref_mod_curatability.all()
    references_with_curatability = []
    for wft in wfts:
        references_with_curatability.append(wft.reference_id)

    query_prepub_references = db.query(
        ReferenceModel
    ).filter_by(
        prepublication_pipeline=True
    )
    pp_refs = query_prepub_references.all()
    references_with_prepublication = []
    for pp_ref in pp_refs:
        references_with_prepublication.append(pp_ref.reference_id)

    # filtering by prepublication_pipeline and then sorting at the end takes 2.5 seconds,
    # doing a separate query for prepub references and filtering by that takes 1 second.
    references_query = db.query(
        ReferenceModel
    ).filter(
        ReferenceModel.reference_id.in_(references_with_prepublication)
    ).join(
        ReferenceModel.mod_corpus_association
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModModel.abbreviation == mod_abbreviation
    ).join(
        CrossReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id
    ).filter(
        CrossReferenceModel.curie_prefix == 'PMID'  # noqa
    ).outerjoin(
        ReferenceModel.copyright_license
    ).filter(
        ReferenceModel.reference_id.notin_(references_with_curatability)
    ).order_by(
        ReferenceModel.curie.desc()
    ).limit(count)
    references = references_query.all()
    return show_sort_result(references, mod_abbreviation, db)


def get_referencefile_mod(referencefile_id, db: Session):
    referencefile_mod_query = db.query(
        ReferencefileModAssociationModel
    ).filter(
        ReferencefileModAssociationModel.referencefile_id == referencefile_id
    )
    referencefile_mod = referencefile_mod_query.all()
    mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db.query(ModModel).all()])
    return [
        ReferencefileModSchemaShow(
            referencefile_id=rfm.referencefile_id, referencefile_mod_id=rfm.referencefile_mod_id,
            mod_abbreviation=mod_id_to_mod.get(rfm.mod_id, '')) for rfm in referencefile_mod]


def get_mod_curators(db: Session, mod_abbreviation):

    _, one_month_ago, _ = get_past_to_present_date_range(30)

    sql_query_str = """
    SELECT DISTINCT
        u.id,
        e.email_address AS email,
        p.display_name AS name
    FROM mod_corpus_association mca
    JOIN mod m ON m.mod_id = mca.mod_id
    LEFT JOIN users u ON u.id = mca.updated_by
    LEFT JOIN person p ON p.person_id = u.person_id
    LEFT JOIN email e ON e.person_id = u.person_id
    WHERE mca.corpus IS NOT NULL
    AND m.abbreviation = :mod_abbreviation
    AND mca.date_updated >= :one_month_ago
    AND p.display_name IS NOT NULL
    """
    sql_query = text(sql_query_str)
    result = db.execute(sql_query, {
        'mod_abbreviation': mod_abbreviation,
        'one_month_ago': one_month_ago
    }).fetchall()
    return {row[2]: row[1] for row in result}, {row[1]: row[0] for row in result}


def get_recently_sorted_reference_ids(db: Session, mod_abbreviation, count, curator_id, day):

    _, start_date, end_date = get_past_to_present_date_range(day)

    sql_query_str = """
        SELECT DISTINCT mcav.reference_id, mcav.date_updated
        FROM mod_corpus_association_version mcav
        INNER JOIN mod m ON m.mod_id = mcav.mod_id
        WHERE m.abbreviation = :mod_abbreviation
          AND (mcav.corpus = TRUE or mcav.corpus = FALSE)
          AND mcav.corpus_mod = TRUE
          AND mcav.operation_type IN (0, 1)
          AND mcav.date_updated >= :start_date
          AND mcav.date_updated < :end_date
    """

    params = {
        "mod_abbreviation": mod_abbreviation,
        "start_date": start_date,
        "end_date": end_date
    }

    if curator_id is not None:
        sql_query_str += " AND mcav.updated_by = :curator_id"
        params["curator_id"] = curator_id

    sql_query_str += " ORDER BY mcav.date_updated DESC"

    if count is not None:
        sql_query_str += " LIMIT :result_limit"
        params["result_limit"] = count

    sql_query = text(sql_query_str)
    rows = db.execute(sql_query, params)
    reference_ids = [row[0] for row in rows]
    return reference_ids


def show_recently_sorted(db: Session, mod_abbreviation, count, curator_email, day):

    name_to_email_mapping, email_to_id_mapping = get_mod_curators(db, mod_abbreviation)

    reference_ids = get_recently_sorted_reference_ids(db, mod_abbreviation, count,
                                                      email_to_id_mapping.get(curator_email),
                                                      day)

    references_query = (
        db.query(ReferenceModel)
        .join(ReferenceModel.mod_corpus_association)
        .filter(ModCorpusAssociationModel.corpus.in_([True, False]))
        .join(ModCorpusAssociationModel.mod)
        .filter(ModModel.abbreviation == mod_abbreviation)
        .outerjoin(ReferenceModel.copyright_license)
        .filter(ReferenceModel.reference_id.in_(reference_ids))
        .order_by(ModCorpusAssociationModel.date_updated)
    )
    references = references_query.all()
    data = show_sort_result(references, mod_abbreviation, db)
    return {
        "curator_data": name_to_email_mapping,
        "data": data
    }
