# AGR Literature Markdown Schema

This document defines the canonical Markdown format produced by the `xml2md`
pipeline and expected by downstream consumers (including the consensus-merge
output from `agr_pdf_extraction_service`).

## Document Structure

A valid document follows this top-level order:

```
# Title                          ← exactly one H1 (required)
                                 ← blank line
Author A, Author B               ← plain comma-separated names (optional)
                                 ← blank line
## Abstract                      ← abstract section (optional but recommended)
                                 ← blank line
Paragraph text ...

**Keywords:** kw1, kw2           ← inline keyword list (optional)
                                 ← blank line
## Section Heading               ← body sections at H2 level
                                 ← blank line
Paragraph text ...

## Acknowledgments               ← before References (optional)
                                 ← blank line
## References                    ← last H2 section (optional)
```

## Heading Hierarchy

- **H1 (`#`)** — Document title. Exactly one per document; must be the first
  heading.
- **H2 (`##`)** — Top-level sections: Abstract, body sections, Acknowledgments,
  References.
- **H3–H6 (`###`–`######`)** — Subsections. No heading level may be skipped
  (e.g., H2 followed directly by H4 without an intervening H3 is invalid).
- Maximum heading depth is H6 (`######`).

## Block Elements

All block elements (paragraphs, headings, tables, lists, figures, formulas,
footnotes) must be followed by a blank line.

### Paragraphs

Plain text. Inline citations may appear as `[1]`, `[1, 2]`, or
`(Author, Year)`.

### Figures

```
**Figure 1.** Caption text describing the figure.
```

### Tables (GFM)

```
| Header 1 | Header 2 |
|---|---|
| data 1   | data 2   |

**Table 1.** Caption text.
```

- Pipe characters inside cells must be escaped as `\|`.
- Newlines inside cells are replaced with spaces.
- Every table must have a header row, a `|---|` separator row, and at least
  one data row (or zero data rows if only the header is meaningful).

### Formulas

Plain text on its own line (no blockquote wrapping):

```
E = mc^2
```

### Lists

Unordered:
```
- Item one
- Item two
```

Ordered:
```
1. First item
2. Second item
```

### Footnotes

```
[^1]: Footnote text here.
```

### References

Numbered list in the final `## References` section:

```
1. Author A, Author B (2024) Title of the paper. *Journal Name*, 52(3), 100-110. doi:10.1234/example
```

## Machine-Checkable Validation Rules

| ID  | Rule                                                         | Severity |
|-----|--------------------------------------------------------------|----------|
| S01 | Exactly one H1 heading                                       | error    |
| S02 | H1 must be the first heading in the document                 | error    |
| S03 | No heading level skips (e.g., H2 → H4 without H3)           | warning  |
| S04 | `## Abstract` appears before body sections                   | warning  |
| S05 | `## References` is the last H2 section                       | warning  |
| S06 | `## Acknowledgments` before `## References`                  | warning  |
| S07 | GFM tables have header row + `|---|` separator + data rows   | error    |
| S08 | Block elements are followed by blank lines                   | warning  |
| S09 | Document ends with exactly one trailing newline               | warning  |

## Notes

- Section numbers (e.g., "1.", "2.1") are **omitted** from headings.
- Author affiliations are stored in the data model but not emitted in
  Markdown.
- The schema is intentionally format-agnostic regarding the XML source
  (TEI or JATS) — both must produce conforming output.
