from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class ConversionFileInfo(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    display_name: str
    file_class: str
    referencefile_id: Optional[int] = None


class ConversionPerFileProgressSchema(BaseModel):
    """One entry per source file eligible for conversion on the most recent
    job, plus one entry per converted Markdown row currently in the DB for
    which no job entry exists. ``status`` is ``pending`` (not yet attempted),
    ``success``, or ``failed``. For ``pending`` and ``success`` entries,
    ``converted`` contains the (expected or actual) output file's
    display_name and file_class; for ``failed`` entries ``converted`` is
    null and ``error`` is populated. ``source`` may be null for entries
    derived purely from the DB — call ``show_all`` if you need the original
    source file info for those."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    source: Optional[ConversionFileInfo] = None
    converted: Optional[ConversionFileInfo] = None
    status: str
    error: Optional[str] = None


class ConversionStatusResponseSchema(BaseModel):
    """
    Response for GET /reference/referencefile/conversion_request/{curie_or_reference_id}.

    Reports the conversion state for a reference and includes per-file progress
    for the most recent job (if any). Callers use the existing
    GET /reference/referencefile/show_all/{curie_or_reference_id} to retrieve
    the file listing once ``status`` is ``converted``.

    Possible ``status`` values:
        - ``converted`` — every convertible source has a converted Markdown
          row in the DB (whether produced by this call or a prior one).
        - ``running``   — a conversion job is currently in progress.
        - ``failed``    — the most recent conversion failed; see
          ``error_message`` and ``per_file_progress``.
        - ``no_sources``— the reference has nothing to convert.

    ``converted_classes`` lists the converted file_class values currently
    present in the DB for this reference. Clients can act on partial results
    (fetching ``converted_merged_main`` as soon as it appears, for example,
    without waiting for longer-running supplement conversions to finish).
    """
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    reference_curie: str
    status: str
    job_id: Optional[str] = None
    error_message: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    converted_classes: List[str] = []
    per_file_progress: List[ConversionPerFileProgressSchema] = []
