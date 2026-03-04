"""Validate Markdown documents against the AGR Literature Markdown Schema.

Pure Python module with no FastAPI dependencies — importable by both
agr_literature_service and agr_pdf_extraction_service.

See MARKDOWN_SCHEMA.md for the full specification.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    """Severity level for validation issues."""
    ERROR = "error"
    WARNING = "warning"


@dataclass
class ValidationIssue:
    """A single validation finding."""
    rule_id: str
    severity: Severity
    line: int
    message: str


@dataclass
class ValidationResult:
    """Aggregate result of validating a Markdown document."""
    valid: bool = True
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------
_RE_HEADING = re.compile(r"^(#{1,6})\s+(.*)$")
_RE_TABLE_SEP = re.compile(r"^\|(?:\s*-{3,}\s*\|)+\s*$")
_RE_TABLE_ROW = re.compile(r"^\|.*\|$")
_RE_BLOCK_ELEMENT = re.compile(
    r"^("
    r"#{1,6}\s"           # heading
    r"|\|.*\|"            # table row
    r"|- "                # unordered list item
    r"|\d+\.\s"           # ordered list item
    r"|\*\*Figure\s"      # figure caption
    r"|\*\*Table\s"       # table caption
    r"|\[\^"              # footnote
    r")"
)


def validate_markdown(text: str) -> ValidationResult:
    """Validate *text* against the AGR Literature Markdown Schema.

    Args:
        text: The full Markdown document as a string.

    Returns:
        A :class:`ValidationResult` with any errors and warnings.
    """
    result = ValidationResult()
    lines = text.split("\n") if text else []

    _check_s01(lines, result)
    _check_s02(lines, result)
    _check_s03(lines, result)
    _check_s04(lines, result)
    _check_s05(lines, result)
    _check_s06(lines, result)
    _check_s07(lines, result)
    _check_s08(lines, result)
    _check_s09(text, result)

    result.valid = len(result.errors) == 0
    return result


# ---------------------------------------------------------------------------
# Individual rule checkers
# ---------------------------------------------------------------------------

def _headings(lines: list[str]) -> list[tuple[int, int, str]]:
    """Return ``[(line_number, level, text), ...]`` for all headings."""
    out: list[tuple[int, int, str]] = []
    for i, line in enumerate(lines, 1):
        m = _RE_HEADING.match(line)
        if m:
            out.append((i, len(m.group(1)), m.group(2)))
    return out


def _check_s01(lines: list[str], result: ValidationResult) -> None:
    """S01: Exactly one H1 heading."""
    h1s = [(ln, txt) for ln, lvl, txt in _headings(lines) if lvl == 1]
    if len(h1s) == 0:
        result.errors.append(ValidationIssue(
            rule_id="S01", severity=Severity.ERROR, line=1,
            message="Document has no H1 heading (expected exactly one)",
        ))
    elif len(h1s) > 1:
        for ln, _ in h1s[1:]:
            result.errors.append(ValidationIssue(
                rule_id="S01", severity=Severity.ERROR, line=ln,
                message="Multiple H1 headings found (expected exactly one)",
            ))


def _check_s02(lines: list[str], result: ValidationResult) -> None:
    """S02: H1 must be the first heading."""
    headings = _headings(lines)
    if headings and headings[0][1] != 1:
        result.errors.append(ValidationIssue(
            rule_id="S02", severity=Severity.ERROR, line=headings[0][0],
            message=(
                f"First heading is H{headings[0][1]} "
                f"('{headings[0][2]}'), expected H1"
            ),
        ))


def _check_s03(lines: list[str], result: ValidationResult) -> None:
    """S03: No heading level skips (e.g., H2 → H4 without H3)."""
    headings = _headings(lines)
    for i in range(1, len(headings)):
        prev_level = headings[i - 1][1]
        curr_level = headings[i][1]
        if curr_level > prev_level + 1:
            result.warnings.append(ValidationIssue(
                rule_id="S03", severity=Severity.WARNING,
                line=headings[i][0],
                message=(
                    f"Heading level skipped: H{prev_level} → H{curr_level} "
                    f"('{headings[i][2]}')"
                ),
            ))


def _check_s04(lines: list[str], result: ValidationResult) -> None:
    """S04: ``## Abstract`` appears before body sections."""
    h2s = [(ln, txt) for ln, lvl, txt in _headings(lines) if lvl == 2]
    if not h2s:
        return
    abstract_indices = [
        i for i, (_, txt) in enumerate(h2s)
        if txt.strip().lower() == "abstract"
    ]
    if not abstract_indices:
        return
    # Abstract should be the first H2
    if abstract_indices[0] != 0:
        result.warnings.append(ValidationIssue(
            rule_id="S04", severity=Severity.WARNING,
            line=h2s[abstract_indices[0]][0],
            message="'## Abstract' is not the first H2 section",
        ))


def _check_s05(lines: list[str], result: ValidationResult) -> None:
    """S05: ``## References`` is the last H2 section."""
    h2s = [(ln, txt) for ln, lvl, txt in _headings(lines) if lvl == 2]
    if not h2s:
        return
    ref_indices = [
        i for i, (_, txt) in enumerate(h2s)
        if txt.strip().lower() == "references"
    ]
    if not ref_indices:
        return
    if ref_indices[-1] != len(h2s) - 1:
        result.warnings.append(ValidationIssue(
            rule_id="S05", severity=Severity.WARNING,
            line=h2s[ref_indices[-1]][0],
            message="'## References' is not the last H2 section",
        ))


def _check_s06(lines: list[str], result: ValidationResult) -> None:
    """S06: ``## Acknowledgments`` appears before ``## References``."""
    h2s = [(ln, txt) for ln, lvl, txt in _headings(lines) if lvl == 2]
    if not h2s:
        return
    ack_idx: int | None = None
    ref_idx: int | None = None
    for i, (_, txt) in enumerate(h2s):
        lower = txt.strip().lower()
        if lower == "acknowledgments" and ack_idx is None:
            ack_idx = i
        if lower == "references" and ref_idx is None:
            ref_idx = i
    if ack_idx is not None and ref_idx is not None and ack_idx > ref_idx:
        result.warnings.append(ValidationIssue(
            rule_id="S06", severity=Severity.WARNING,
            line=h2s[ack_idx][0],
            message=(
                "'## Acknowledgments' appears after '## References' "
                "(should come before)"
            ),
        ))


def _check_s07(lines: list[str], result: ValidationResult) -> None:
    """S07: GFM tables have header + ``|---|`` separator + data rows."""
    i = 0
    while i < len(lines):
        if _RE_TABLE_ROW.match(lines[i]) and not _RE_TABLE_SEP.match(lines[i]):
            # Found a potential table header row
            table_start = i + 1  # 1-indexed
            if i + 1 >= len(lines) or not _RE_TABLE_SEP.match(lines[i + 1]):
                result.errors.append(ValidationIssue(
                    rule_id="S07", severity=Severity.ERROR,
                    line=table_start,
                    message=(
                        "Table row without separator: header row must be "
                        "followed by a |---| separator line"
                    ),
                ))
            # Skip past the rest of this table
            i += 1
            while i < len(lines) and (
                _RE_TABLE_ROW.match(lines[i])
                or _RE_TABLE_SEP.match(lines[i])
            ):
                i += 1
        else:
            i += 1


def _check_s08(lines: list[str], result: ValidationResult) -> None:
    """S08: Block elements are followed by blank lines."""
    for i, line in enumerate(lines):
        if not _RE_BLOCK_ELEMENT.match(line):
            continue
        # Skip table rows that are followed by more table rows or separators
        if _RE_TABLE_ROW.match(line) or _RE_TABLE_SEP.match(line):
            next_idx = i + 1
            if next_idx < len(lines) and (
                _RE_TABLE_ROW.match(lines[next_idx])
                or _RE_TABLE_SEP.match(lines[next_idx])
            ):
                continue
        # Skip list items followed by more list items
        if re.match(r"^- ", line) or re.match(r"^\d+\.\s", line):
            next_idx = i + 1
            if next_idx < len(lines) and (
                re.match(r"^- ", lines[next_idx])
                or re.match(r"^\d+\.\s", lines[next_idx])
            ):
                continue
        # Skip footnotes followed by more footnotes
        if line.startswith("[^"):
            next_idx = i + 1
            if next_idx < len(lines) and lines[next_idx].startswith("[^"):
                continue
        # Now check that next line is blank (or end-of-file)
        next_idx = i + 1
        if next_idx < len(lines) and lines[next_idx].strip() != "":
            result.warnings.append(ValidationIssue(
                rule_id="S08", severity=Severity.WARNING,
                line=i + 1,
                message=(
                    "Block element not followed by a blank line"
                ),
            ))


def _check_s09(text: str, result: ValidationResult) -> None:
    """S09: Document ends with exactly one trailing newline."""
    if not text:
        result.warnings.append(ValidationIssue(
            rule_id="S09", severity=Severity.WARNING, line=1,
            message="Document is empty",
        ))
        return
    if not text.endswith("\n"):
        result.warnings.append(ValidationIssue(
            rule_id="S09", severity=Severity.WARNING,
            line=len(text.split("\n")),
            message="Document does not end with a trailing newline",
        ))
    elif text.endswith("\n\n"):
        result.warnings.append(ValidationIssue(
            rule_id="S09", severity=Severity.WARNING,
            line=len(text.split("\n")),
            message="Document ends with multiple trailing newlines",
        ))
