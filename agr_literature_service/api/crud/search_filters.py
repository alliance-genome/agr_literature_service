from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, time, timezone


def ensure_filter_structure(es_body: Dict[str, Any]) -> None:
    """
    Ensure es_body has the nested structure where we attach range filters:
      es_body["query"]["bool"]["filter"]["bool"]["must"] (list)
    """
    q = es_body.setdefault("query", {}).setdefault("bool", {})
    f = q.setdefault("filter", {}).setdefault("bool", {})
    f.setdefault("must", [])


def date_str_to_micro_seconds(date_str: str, start: bool) -> int:
    """
    Convert a date string (YYYY-MM-DD or ISO 8601 with time) to a UTC timestamp in MICROseconds,
    normalized to the start or end of day.

    - start=True  -> 00:00:00.000000Z
    - start=False -> 23:59:59.999000Z (just under midnight)

    ES stores our `date_created` as an epoch-in-microseconds long;
    this avoids local timezone drift and keeps comparisons consistent.
    """
    # Parse to date-only, ignoring any provided local time component
    d: date = datetime.fromisoformat(date_str).date()
    t: time = time(0, 0, 0) if start else time(23, 59, 59, 999000)
    dt = datetime.combine(d, t).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def add_pubmed_modified_range(
    es_body: Dict[str, Any],
    date_pubmed_modified: Optional[Tuple[str, str]],
) -> None:
    if not date_pubmed_modified:
        return
    ensure_filter_structure(es_body)
    start, end = date_pubmed_modified
    es_body["query"]["bool"]["filter"]["bool"]["must"].append({
        "range": {"date_last_modified_in_pubmed": {"gte": start, "lte": end}}
    })


def add_pubmed_arrive_range(
    es_body: Dict[str, Any],
    date_pubmed_arrive: Optional[Tuple[str, str]],
) -> None:
    if not date_pubmed_arrive:
        return
    ensure_filter_structure(es_body)
    start, end = date_pubmed_arrive
    es_body["query"]["bool"]["filter"]["bool"]["must"].append({
        "range": {"date_arrived_in_pubmed": {"gte": start, "lte": end}}
    })


def add_created_range(
    es_body: Dict[str, Any],
    date_created: Optional[Tuple[str, str]],
) -> None:
    """
    date_created is stored as epoch-in-microseconds in ES.
    We normalize the incoming strings to UTC start/end of day and convert.
    """
    if not date_created:
        return
    ensure_filter_structure(es_body)
    start_s, end_s = date_created
    es_body["query"]["bool"]["filter"]["bool"]["must"].append({
        "range": {
            "date_created": {
                "gte": date_str_to_micro_seconds(start_s, True),
                "lte": date_str_to_micro_seconds(end_s, False),
            }
        }
    })


def add_published_interval_overlap(
    es_body: Dict[str, Any],
    date_published: Optional[Tuple[str, str]],
) -> None:
    """
    Model 'overlap' against a [start, end] window when documents store
    date_published_start / date_published_end (ISO dates).

      Overlap if:
        - end within window OR
        - start within window OR
        - (start <= window_start AND end >= window_end)
    """
    if not date_published:
        return
    ensure_filter_structure(es_body)
    start, end = date_published
    es_body["query"]["bool"]["filter"]["bool"]["must"].append({
        "bool": {
            "should": [
                {"range": {"date_published_end": {"gte": start, "lte": end}}},
                {"range": {"date_published_start": {"gte": start, "lte": end}}},
                {
                    "bool": {
                        "must": [
                            {"range": {"date_published_start": {"lte": start}}},
                            {"range": {"date_published_end": {"gte": end}}},
                        ]
                    }
                },
            ]
        }
    })


def apply_all_date_filters(
    es_body: Dict[str, Any],
    *,
    date_pubmed_modified: Optional[List[str]] = None,
    date_pubmed_arrive: Optional[List[str]] = None,
    date_published: Optional[List[str]] = None,
    date_created: Optional[List[str]] = None,
) -> bool:
    """
    Convenience wrapper used by search_crud:
      - Mutates es_body in place with the relevant range filters.
      - Returns True if at least one date filter was applied.
    """
    applied_any = False

    if date_pubmed_modified:
        add_pubmed_modified_range(es_body, (date_pubmed_modified[0], date_pubmed_modified[1]))
        applied_any = True

    if date_pubmed_arrive:
        add_pubmed_arrive_range(es_body, (date_pubmed_arrive[0], date_pubmed_arrive[1]))
        applied_any = True

    if date_created:
        add_created_range(es_body, (date_created[0], date_created[1]))
        applied_any = True

    if date_published:
        add_published_interval_overlap(es_body, (date_published[0], date_published[1]))
        applied_any = True

    return applied_any
