"""Unit tests for tidying search highlight fragments (SCRUM-6490).

The fragments are taken from stage results for the query "DNA Damage".
"""
from agr_literature_service.api.crud.search_crud import merge_highlight_fragments, remap_highlights

CITATION = ("Singh J; Patten SA, (2026) PAICS mediates DNA damage and cerebellar neuronal loss "
            "in C9orf72 amyotrophic lateral sclerosis.")


def test_split_sentence_is_joined_back():
    fragments = ["Singh J; Patten SA, (2026) PAICS mediates <em>DNA</em>",
                 "<em>damage</em> and cerebellar neuronal loss in C9orf72 amyotrophic lateral sclerosis."]
    assert merge_highlight_fragments(fragments, CITATION) == [
        "Singh J; Patten SA, (2026) PAICS mediates <em>DNA</em> <em>damage</em> and cerebellar neuronal loss "
        "in C9orf72 amyotrophic lateral sclerosis."
    ]


def test_separate_passages_stay_separate():
    abstract = ("Widespread DNA damage and repair defects. Many other words here. "
                "Restoring paics expression resolves DNA damage.")
    fragments = ["Widespread <em>DNA</em> <em>damage</em> and repair defects.",
                 "Restoring paics expression resolves <em>DNA</em> <em>damage</em>."]
    assert merge_highlight_fragments(fragments, abstract) == fragments


def test_only_adjacent_pieces_are_joined():
    abstract = "First part of a long sentence about DNA damage and repair. Unrelated. Later DNA mention."
    fragments = ["First part of a long sentence about <em>DNA</em>",
                 "<em>damage</em> and repair.",
                 "Later <em>DNA</em> mention."]
    assert merge_highlight_fragments(fragments, abstract) == [
        "First part of a long sentence about <em>DNA</em> <em>damage</em> and repair.",
        "Later <em>DNA</em> mention.",
    ]


def test_repeated_mesh_headings_are_dropped():
    fragments = ["<em>DNA</em>", "<em>DNA</em>", "<em>DNA</em> <em>Damage</em>"]
    assert merge_highlight_fragments(fragments) == ["<em>DNA</em>", "<em>DNA</em> <em>Damage</em>"]


def test_fragment_missing_from_source_is_kept_as_is():
    fragments = ["<em>DNA</em> one", "<em>damage</em> two"]
    assert merge_highlight_fragments(fragments, "nothing matches here") == fragments


def test_remap_highlights_uses_each_fields_own_source():
    highlights = {
        "citation": ["Singh J; Patten SA, (2026) PAICS mediates <em>DNA</em>",
                     "<em>damage</em> and cerebellar neuronal loss in C9orf72 amyotrophic lateral sclerosis."],
        "mesh_terms.heading_term": ["<em>DNA</em> <em>Damage</em>", "<em>DNA</em> <em>Damage</em>"],
    }
    source = {"citation": CITATION, "mesh_terms": [{"heading_term": "DNA Damage"}]}
    remapped = remap_highlights(highlights, source)
    assert len(remapped["citation"]) == 1
    assert remapped["mesh"] == ["<em>DNA</em> <em>Damage</em>"]
