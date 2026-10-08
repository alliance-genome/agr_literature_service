"""SCRUM-6513: per-paper attribution of PubMed affiliation emails to authors.
The cases are real ones from the first backfill (dev, 2026-10-08)."""
from agr_literature_service.lit_processing.data_ingest.utils.author_email_utils import (
    assign_author_emails,
    email_matches_author,
    emails_in_affiliations,
)


def test_emails_in_affiliations_all_in_order_once():
    affiliations = ["Dept X, Kyoto, Japan. Electronic address: John.Doe@Kyoto-U.ac.jp.",
                    "Lab Y. mayerm@mail.nih.gov mihaela.serpe@nih.gov.", "Again john.doe@kyoto-u.ac.jp",
                    "contact <x_y@foo.co.uk>", "user@localhost"]
    assert emails_in_affiliations(affiliations) == [
        "john.doe@kyoto-u.ac.jp", "mayerm@mail.nih.gov", "mihaela.serpe@nih.gov", "x_y@foo.co.uk"]
    assert emails_in_affiliations(None) == []


def test_email_matches_author_name_forms():
    assert email_matches_author("mayerm@mail.nih.gov", "Mark L", "Mayer")
    assert email_matches_author("pe.cieslak@uj.edu.pl", "Przemyslaw E", "Cieslak")
    assert email_matches_author("dpayetbornet@x.fr", "Dominique", "Payet-Bornet")
    assert email_matches_author("xuzg@sdu.edu.cn", "Zhigang", "Xu")          # short last name + initials
    assert email_matches_author("yongx@bcm.edu", "Yong", "Xu")               # first name
    assert email_matches_author("mueller@x.de", "Anna", "Müller") is False   # accents fold to "muller"
    assert email_matches_author("muller@x.de", "Anna", "Müller")
    assert not email_matches_author("lisa.smith@x.org", "Wei", "Li")         # "li" prefix of a long local part
    assert not email_matches_author("isf20@cam.ac.uk", "I Sadaf", "Farooqi")


def _authors(*rows):
    return [(first, last, affs) for first, last, affs in rows]


def test_several_addresses_in_one_affiliation_go_to_their_owners():
    # Serpe's affiliation lists Mayer's address first, then hers
    emails = assign_author_emails(_authors(
        ("Mark L", "Mayer", ["NICHD, NIH. mayerm@mail.nih.gov"]),
        ("Mihaela", "Serpe", ["Program in Cellular Regulation and Metabolism and mayerm@mail.nih.gov "
                              "mihaela.serpe@nih.gov."]),
    ))
    assert emails == ["mayerm@mail.nih.gov", "mihaela.serpe@nih.gov"]


def test_shared_affiliation_goes_only_to_the_named_author():
    shared = ["Institute of Zoology, Jagiellonian University, Krakow. pe.cieslak@uj.edu.pl"]
    emails = assign_author_emails(_authors(
        ("Przemyslaw E", "Cieslak", shared),
        ("Anna", "Blasiak", shared),
    ))
    assert emails == ["pe.cieslak@uj.edu.pl", None]


def test_shared_address_matching_nobody_goes_to_nobody():
    shared = ["Lab Z. lab-office@uni.edu"]
    assert assign_author_emails(_authors(("Ann", "Lee", shared), ("Bo", "Kim", shared))) == [None, None]


def test_single_unshared_address_is_kept_even_without_a_name_match():
    emails = assign_author_emails(_authors(
        ("I Sadaf", "Farooqi", ["Addenbrooke's Hospital, Cambridge. isf20@cam.ac.uk."]),
        ("Yong", "Xu", ["Baylor College of Medicine. yongx@bcm.edu.", "Houston. yongx@bcm.edu."]),
        ("Ann", "Lee", ["No address here"]),
    ))
    assert emails == ["isf20@cam.ac.uk", "yongx@bcm.edu", None]


def test_address_naming_a_coauthor_is_not_given_to_the_lister():
    # Payet-Bornet's affiliation carries Asnafi's address only
    emails = assign_author_emails(_authors(
        ("Dominique", "Payet-Bornet", ["CIML, Marseille. vahid.asnafi@nck.aphp.fr"]),
        ("Vahid", "Asnafi", ["INEM, Paris. vahid.asnafi@nck.aphp.fr"]),
    ))
    assert emails == [None, "vahid.asnafi@nck.aphp.fr"]


def test_two_unmatched_addresses_are_ambiguous():
    emails = assign_author_emails(_authors(("Ann", "Lee", ["X. a1@x.org; b2@y.org"])))
    assert emails == [None]


def test_full_last_name_beats_part_of_a_compound_last_name():
    # 34530988: Raffaella De Paolo (author 9) and Paolo Sordino (author 19)
    emails = assign_author_emails(_authors(
        ("Raffaella", "De Paolo", ["Stazione Zoologica Anton Dohrn, Naples."]),
        ("Paolo", "Sordino", ["Stazione Zoologica Anton Dohrn, Naples. Electronic address: paolo.sordino@szn.it."]),
    ))
    assert emails == [None, "paolo.sordino@szn.it"]


def test_equally_good_name_matches_are_ambiguous():
    shared = ["Dept. wang@uni.edu"]
    assert assign_author_emails(_authors(("Li", "Wang", shared), ("Hui", "Wang", shared))) == [None, None]


def test_first_name_beats_a_short_last_name():
    # review note: Wei Li must not take Lisa Wong's "lisa@" address
    shared = ["X lisa@u.edu"]
    assert assign_author_emails(_authors(("Lisa", "Wong", shared), ("Wei", "Li", shared))) == ["lisa@u.edu", None]
    # a short last name still wins when nothing better names the address
    assert assign_author_emails(_authors(("Zhigang", "Xu", ["Shandong. xuzg@sdu.edu.cn"]),
                                         ("Wei", "Xiong", ["Beijing."]))) == ["xuzg@sdu.edu.cn", None]
