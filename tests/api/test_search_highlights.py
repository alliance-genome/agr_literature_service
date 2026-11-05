"""Unit tests for tidying search highlight fragments (SCRUM-6490).

The fragments are taken from stage results for the query "DNA Damage".
"""
from agr_literature_service.api.crud.search_crud import best_mesh_headings, merge_highlight_fragments, \
    remap_highlights, strip_markup

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


def test_markup_from_the_source_is_dropped():
    # an abstract stored with HTML: the fragment starts inside "<p>"
    assert strip_markup("p><strong>Purpose</strong>: reduce <em>Hirschsprung</em> <em>disease</em> (HSCR)</p><p>") \
        == "Purpose: reduce <em>Hirschsprung</em> <em>disease</em> (HSCR)"
    assert strip_markup("<p><strong>Background</strong>: <em>Hirschsprung</em> disease is a congenital</stro") \
        == "Background: <em>Hirschsprung</em> disease is a congenital"
    assert strip_markup("variants of <i>DSCAM</i> and <i>BACE2</i> with <em>Hirschsprung</em>") \
        == "variants of DSCAM and BACE2 with <em>Hirschsprung</em>"
    # text without markup, and a comparison that is not a tag, are unchanged
    assert strip_markup("ratio > 1 in <em>DNA</em> <em>damage</em>") == "ratio > 1 in <em>DNA</em> <em>damage</em>"
    assert strip_markup('p class="abstract">The <em>DNA</em> damage') == "The <em>DNA</em> damage"
    assert strip_markup("<em>DNA</em> damage rose (p < 0.05") == "<em>DNA</em> damage rose (p < 0.05"
    assert strip_markup("<em>DNA</em> damage in the <") == "<em>DNA</em> damage in the"


def test_only_mesh_headings_matching_the_most_words_are_kept():
    fragments = ["<em>Disease</em> Models, Animal", "Genetic Predisposition to <em>Disease</em>",
                 "<em>Hirschsprung</em> <em>Disease</em>"]
    assert best_mesh_headings(fragments) == ["<em>Hirschsprung</em> <em>Disease</em>"]
    # a one-word query: every heading matches one word, all are kept
    fragments = ["<em>DNA</em> Damage", "<em>DNA</em> Repair"]
    assert best_mesh_headings(fragments) == fragments
    assert best_mesh_headings([]) == []


def test_remap_highlights_cleans_markup_and_mesh():
    abstract = "<p><strong>Purpose</strong>: To determine whether folate reduces Hirschsprung disease.</p>"
    highlights = {
        "abstract": ["p><strong>Purpose</strong>: To determine whether folate reduces "
                     "<em>Hirschsprung</em> <em>disease</em>.</p>"],
        "mesh_terms.heading_term": ["<em>Hirschsprung</em> <em>Disease</em>", "<em>Disease</em> Models, Animal"],
    }
    remapped = remap_highlights(highlights, {"abstract": abstract})
    assert remapped["abstract"] == ["Purpose: To determine whether folate reduces "
                                    "<em>Hirschsprung</em> <em>disease</em>."]
    assert remapped["mesh"] == ["<em>Hirschsprung</em> <em>Disease</em>"]


def test_tag_piece_regex_does_not_backtrack():
    # CodeQL: an attribute run like 'A -="" -="" ...' made LEADING_TAG_PIECE backtrack
    # exponentially; with 40 repeats the old pattern would not finish.
    fragment = "A -=" + '"" -=' * 40 + "<em>DNA</em>"
    assert strip_markup(fragment).endswith("<em>DNA</em>")
