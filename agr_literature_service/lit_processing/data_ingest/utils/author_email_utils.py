"""
SCRUM-6513: attribute the emails in PubMed affiliation strings to authors.

PubMed puts an author's email inside that author's AffiliationInfo, usually as
"... Electronic address: name@domain.org." Taking the first address in an
author's affiliations is wrong in two common cases (seen in the first
backfill: ~1,650 of 34,741 emails went to a co-author):

- one affiliation lists several addresses ("... mayerm@mail.nih.gov
  mihaela.serpe@nih.gov."), so the first one may belong to someone else;
- older records attach the same affiliation text, with the corresponding
  author's address, to several co-authors.

So emails are assigned per paper, looking at all of its authors together
(``assign_author_emails``):

1. an address in the author's own affiliations whose local part matches
   this author's name better than any other author's is that author's
   (full last name > part of a compound last name > first name > a 1-2
   letter last name plus initials, so "paolo.sordino" goes to Paolo Sordino,
   not to Raffaella De Paolo, and "lisa" to Lisa Wong, not to Wei Li; a tie
   gives it to nobody);
2. otherwise, an author whose affiliations hold exactly one address keeps
   it, provided no other author lists the same address and it does not
   match another author's name;
3. otherwise the author gets no email (ambiguous).

An address therefore goes to at most one author per paper.
"""
import re
import unicodedata
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Set, Tuple

# The full-text extractor's pattern (extract_emails.py): no address character
# may touch either end, and a '.' only belongs to the address when another
# domain label follows, so a sentence-final period is never swallowed.
AUTHOR_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+-])"
    r"([A-Za-z0-9][A-Za-z0-9._%+-]{0,63}"
    r"@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63})"
    r"(?![A-Za-z0-9_%+-])(?!\.[A-Za-z0-9])",
    re.IGNORECASE,
)

# (first name, last name, affiliations) of one author, in author order.
AuthorNameAffiliations = Tuple[Optional[str], Optional[str], Optional[Sequence[str]]]

# A short last name ("Xu", "Li") only matches a local part that is little more
# than the name plus initials ("xuzg", "lixy"), not any address starting "li".
_SHORT_NAME_MAX_EXTRA = 4
_MIN_NAME_PART = 3
_MIN_FIRST_NAME = 4


def _letters(value: Optional[str]) -> str:
    """Lowercase ASCII letters only, accents removed (Müller -> muller)."""
    decomposed = unicodedata.normalize("NFKD", value or "")
    return re.sub(r"[^a-z]", "", "".join(c for c in decomposed if not unicodedata.combining(c)).lower())


def emails_in_affiliations(affiliations: Optional[Sequence[str]]) -> List[str]:
    """Every address in the affiliation strings, lowercased, in order, once."""
    emails: List[str] = []
    for affiliation in affiliations or []:
        for match in AUTHOR_EMAIL_RE.finditer(affiliation or ""):
            email = match.group(1).rstrip(".").lower()
            if email not in emails:
                emails.append(email)
    return emails


# How well an address's local part carries an author's name. A 1-2 letter last
# name ("Li", "Xu") is the weakest evidence: it is a prefix or suffix of many
# local parts, so a co-author's first name ("lisa") must win over it.
FULL_LAST_NAME = 4
LAST_NAME_PART = 3
FIRST_NAME = 2
SHORT_LAST_NAME = 1
NO_MATCH = 0


def email_match_score(email: str, first_name: Optional[str], last_name: Optional[str]) -> int:
    """FULL_LAST_NAME: the whole last name (3+ letters: "sordino" in
    "paolosordino"); LAST_NAME_PART: one part of a compound last name (3+
    letters: "payet" for Payet-Bornet, "paolo" for De Paolo); FIRST_NAME:
    the first name (4+ letters: "mihaela"); SHORT_LAST_NAME: a 1-2 letter
    last name plus initials ("xuzg" for Xu); else NO_MATCH."""
    local = _letters(email.split("@", 1)[0])
    if not local:
        return NO_MATCH
    last = _letters(last_name)
    if len(last) >= _MIN_NAME_PART and last in local:
        return FULL_LAST_NAME
    parts = {_letters(part) for part in re.split(r"[\s\-']+", last_name or "")}
    if any(len(part) >= _MIN_NAME_PART and part in local for part in parts):
        return LAST_NAME_PART
    first = _letters((first_name or "").split(" ")[0])
    if len(first) >= _MIN_FIRST_NAME and first in local:
        return FIRST_NAME
    if 0 < len(last) < _MIN_NAME_PART and len(local) <= len(last) + _SHORT_NAME_MAX_EXTRA \
            and (local.startswith(last) or local.endswith(last)):
        return SHORT_LAST_NAME
    return NO_MATCH


def email_matches_author(email: str, first_name: Optional[str], last_name: Optional[str]) -> bool:
    """Does the address's local part carry this author's name at all?"""
    return email_match_score(email, first_name, last_name) > NO_MATCH


def _name_owner(email: str, authors: Sequence[AuthorNameAffiliations]) -> Tuple[Optional[int], bool]:
    """(index of the author whose name the address matches best, or None on a
    tie; whether it matches any author's name at all)."""
    scores = [email_match_score(email, first, last) for first, last, _ in authors]
    best = max(scores, default=NO_MATCH)
    if best == NO_MATCH:
        return None, False
    best_authors = [index for index, score in enumerate(scores) if score == best]
    return (best_authors[0] if len(best_authors) == 1 else None), True


def assign_author_emails(authors: Sequence[AuthorNameAffiliations]) -> List[Optional[str]]:
    """The email of each author of one paper (None when there is none or it
    is ambiguous), by the rules in the module docstring."""
    candidates = [emails_in_affiliations(affiliations) for _, _, affiliations in authors]
    listed_by: Dict[str, Set[int]] = defaultdict(set)
    for index, emails in enumerate(candidates):
        for email in emails:
            listed_by[email].add(index)
    owners = {email: _name_owner(email, authors) for email in listed_by}
    assigned: List[Optional[str]] = []
    for index, emails in enumerate(candidates):
        own = [email for email in emails if owners[email][0] == index]
        if own:
            assigned.append(own[0])
        elif len(emails) == 1 and listed_by[emails[0]] == {index} and not owners[emails[0]][1]:
            assigned.append(emails[0])
        else:
            assigned.append(None)
    return assigned
