#!/usr/bin/env python3
"""Parse ZFIN person addresses from text file and extract address components.

Input:
    zfin_users_A.txt - A text dump of ZFIN person records. The first two lines
    are a blank line and a header. Each person entry starts with a line ending
    in "Edit" (tab-separated), where the text before the tab is the person's
    name and identifier (e.g. "Aamar, Emil eaamar"). The lines between two
    "Edit" lines form that person's address block, typically starting with an
    email address followed by free-text address lines. Some entries have no
    address at all (just a name and possibly an email).

Parsing steps:
    1. Split the file into entries on "Edit" delimiter lines.
    2. From each Edit line, extract last name, first name, and identifier.
       The identifier may be a username, a ZDB-PERS ID (e.g. ZDB-PERS-970425-40),
       or an email address.
    3. From the address block, extract the email (either from the identifier on
       the Edit line or from the first line that looks like an email address).
    4. From the remaining address lines, attempt to extract city, state, postal
       code, and country using a series of pattern-matching strategies applied
       in order from most specific to most general:
       a. Check if the last line is a standalone country name (e.g. "USA",
          "Germany", "Japan"). Handles aliases like "UK" -> "United Kingdom",
          "The Netherlands" -> "Netherlands", and typos like "Tunisa" -> "Tunisia".
       b. Check if the last line ends with ", Country" to handle cases like
          "Helsinki, Finland" or "6525GA Nijmegen, The Netherlands".
       c. Check for a standalone postal code line (US ZIP, UK, Polish, Dutch,
          Canadian, Japanese, Swedish, or generic international formats).
       d. Match city/state/postal patterns on the current bottom line:
          - US-style "City, ST" or "City, ST ZIP" (2-letter state codes,
            including Canadian provinces)
          - "City ST ZIP" without comma
          - International "City, PostalCode" with multi-word postal support
          - Reversed "PostalCode City" (e.g. "02-109 Warsaw", "60438 Frankfurt")
          - "Street, PostalCode, City" (e.g. "Einsteinweg 55, 2333 CC, Leiden")
          - Three-part "City, Region, PostalCode"
       e. Scan all lines for embedded "PostalCode City" or "City PostalCode"
          patterns (e.g. "Manchester M13 9PT", "0317 Oslo").
       f. Scan all lines for country names embedded at end after a comma or
          space (e.g. "...734-8553 Japan", "...Verneuil-en-Halatte, France").
       g. Check for country names in parentheses (e.g. "29071 (Spain)").
       h. Detect country-less postal codes by format:
          - Japanese NNN-NNNN (auto-infers Japan)
          - Swedish NNN NN + City
          - French/generic NNNNN + City
          - Indian 6-digit with space or dash (e.g. "New Delhi - 110 025")
       i. Detect malformed Canadian postals where province code runs into the
          postal code (e.g. "ONK1H 8L1" -> state=ON, postal=K1H 8L1).
       j. Detect Brazilian state codes (e.g. "Porto Alegre, RS" -> Brazil).
       k. As a final fallback, infer country from city name using a lookup
          table of ~45 well-known research cities (e.g. Uppsala -> Sweden,
          Osaka -> Japan, Geneva -> Switzerland). If no city was extracted,
          scan all address text for known city names.
    5. Lines not consumed by the extraction steps are preserved as the
       remaining "address" field, joined with " | ".

Output:
    zfin_users_A_parsed.tsv - Tab-delimited file with all 387 entries.
        Columns: last, first, zdb, email, address, city, state, postal_code,
        country, matched (Y/N flag for whether any location data was extracted).
    zfin_users_A_no_address.tsv - Entries that had no address text at all
        (just name/email). Columns: last, first, zdb, email, address.
    zfin_users_A_no_match_address.tsv - Entries that had address text but
        no city/state/postal/country could be extracted (e.g. institution-only
        addresses, foreign street addresses with no city/country indicators).
        Columns: last, first, zdb, email, address.
    zfin_users_A_summary.txt - Counts of matched, no-match, and no-address
        entries (also printed to stdout).
"""

import re
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent
INPUT_FILE = SCRIPT_DIR / "zfin_users_A.txt"
OUTPUT_FILE = SCRIPT_DIR / "zfin_users_A_parsed.tsv"
NO_ADDRESS_FILE = SCRIPT_DIR / "zfin_users_A_no_address.tsv"
NO_MATCH_ADDRESS_FILE = SCRIPT_DIR / "zfin_users_A_no_match_address.tsv"
SUMMARY_FILE = SCRIPT_DIR / "zfin_users_A_summary.txt"

# US states + Canadian provinces
STATE_CODES = {
    # US
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'GU',
    # Canadian provinces
    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE',
    'QC', 'SK', 'YT',
}

# Map lowercase country names/variants to canonical form
COUNTRIES = {}
for _name in [
    'Australia', 'Austria', 'Belgium', 'Brazil', 'Canada', 'Chile',
    'China', 'Colombia', 'Czech Republic', 'Denmark', 'Egypt',
    'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Iceland',
    'India', 'Iran', 'Ireland', 'Israel', 'Italy', 'Japan', 'Jordan',
    'Malaysia', 'Mexico', 'Netherlands', 'New Zealand', 'Norway',
    'Poland', 'Portugal', 'Russia', 'Scotland', 'Singapore',
    'South Korea', 'Spain', 'Sweden', 'Switzerland', 'Taiwan',
    'Thailand', 'Tunisia', 'Turkey', 'United Arab Emirates',
    'United Kingdom', 'USA',
]:
    COUNTRIES[_name.lower()] = _name
# aliases
COUNTRIES['uk'] = 'United Kingdom'
COUNTRIES['the netherlands'] = 'Netherlands'
COUNTRIES['united states'] = 'USA'
COUNTRIES['united-kingdom'] = 'United Kingdom'
COUNTRIES['macau'] = 'Macau'
COUNTRIES['tunisa'] = 'Tunisia'  # typo in source data
COUNTRIES['pakistan'] = 'Pakistan'

# Brazilian state codes
BRAZILIAN_STATES = {
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
    'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
    'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO',
}

# Well-known research cities → country (used as fallback inference)
CITY_TO_COUNTRY = {
    # India
    'bangalore': 'India', 'chennai': 'India', 'new delhi': 'India',
    'mumbai': 'India', 'hyderabad': 'India', 'kolkata': 'India',
    # Japan
    'osaka': 'Japan', 'tokyo': 'Japan', 'sendai': 'Japan',
    'kawasaki': 'Japan', 'kyoto': 'Japan', 'nagoya': 'Japan',
    'hiroshima': 'Japan', 'kobe': 'Japan', 'sapporo': 'Japan',
    'tsukuba': 'Japan', 'yokohama': 'Japan',
    # Sweden
    'uppsala': 'Sweden', 'stockholm': 'Sweden', 'gothenburg': 'Sweden',
    'lund': 'Sweden', 'umea': 'Sweden',
    # France
    'paris': 'France', 'lyon': 'France', 'marseille': 'France',
    'strasbourg': 'France', 'toulouse': 'France', 'nice': 'France',
    # Switzerland
    'geneva': 'Switzerland', 'basel': 'Switzerland', 'zurich': 'Switzerland',
    'lausanne': 'Switzerland', 'bern': 'Switzerland',
    # Other
    'tehran': 'Iran', 'rawalpindi': 'Pakistan', 'lahore': 'Pakistan',
    'santiago': 'Chile', 'porto alegre': 'Brazil', 'sao paulo': 'Brazil',
    'malaga': 'Spain', 'barcelona': 'Spain', 'madrid': 'Spain',
    'ottawa': 'Canada', 'toronto': 'Canada', 'montreal': 'Canada',
    'vancouver': 'Canada',
    'beijing': 'China', 'shanghai': 'China', 'taipei': 'Taiwan',
    'seoul': 'South Korea', 'singapore': 'Singapore',
    'auckland': 'New Zealand', 'wellington': 'New Zealand',
    'dublin': 'Ireland', 'edinburgh': 'Scotland',
}

EMAIL_RE = re.compile(r'^\s*\S+@\S+\.\S+\s*$')
ZDB_RE = re.compile(r'ZDB-PERS-\d+-\d+')
US_ZIP_RE = re.compile(r'^\s*(\d{5}(?:-\d{4})?)\s*\.?\s*$')
# UK postcodes: "CB2 3DY", "SE1 1UL", "S10 2TN", "SW1A 1AA"
UK_POST_RE = re.compile(
    r'^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$', re.IGNORECASE
)
# Polish postcodes: "02-109", "61-614"
POLISH_POST_RE = re.compile(r'^\d{2}-\d{3}$')
# Dutch postcodes: "6525GA", "2333 CC", "3000 CA"
DUTCH_POST_RE = re.compile(r'^\d{4}\s?[A-Z]{2}$', re.IGNORECASE)
# Canadian postcodes: "M5G 0A4", "K1H 8L1"
CANADA_POST_RE = re.compile(
    r'^[A-Z]\d[A-Z]\s?\d[A-Z]\d$', re.IGNORECASE
)
# Japanese postcodes: "565-0871", "980-8575"
JAPAN_POST_RE = re.compile(r'^\d{3}-\d{4}$')
# Swedish postcodes: "751 24", "752 39"
SWEDEN_POST_RE = re.compile(r'^\d{3}\s\d{2}$')
# Generic international: optional prefix + digits, e.g. "H-2101", "N-5020",
# "SE-41345", "D-13125", "F-59655"
INTL_POST_RE = re.compile(
    r'^[A-Z]{0,3}[\s-]?\d{3,6}(?:[-\s]?\d{0,5})?$', re.IGNORECASE
)


def parse_entries(filepath):
    """Parse the text file into a list of (name_portion, address_lines)."""
    lines = filepath.read_text(encoding='utf-8', errors='replace').splitlines()
    entries = []
    current_name = None
    current_addr = []

    for line in lines[2:]:  # skip blank line and header
        stripped = line.rstrip()
        if re.search(r'\tEdit\s*$', stripped) or stripped.endswith('\tEdit'):
            if current_name is not None:
                entries.append((current_name, current_addr))
            name_part = re.sub(r'\s*\tEdit\s*$', '', stripped)
            name_part = re.sub(r'\s*\t\s*Edit\s*$', '', name_part)
            current_name = name_part.strip()
            current_addr = []
        else:
            if current_name is not None and stripped.strip():
                current_addr.append(stripped.strip())

    if current_name is not None:
        entries.append((current_name, current_addr))

    return entries


def extract_name(name_portion):
    """Extract last, first, zdb, and possible email from the name portion."""
    last = first = zdb = name_email = ''

    if ',' not in name_portion:
        last = name_portion.strip()
        return last, first, zdb, name_email

    parts = name_portion.split(',', 1)
    last = parts[0].strip()
    rest = parts[1].strip()

    tokens = rest.split()
    if tokens:
        first = tokens[0]
        identifier = ' '.join(tokens[1:])
        zdb_match = ZDB_RE.search(identifier)
        if zdb_match:
            zdb = zdb_match.group(0)
        elif '@' in identifier:
            name_email = identifier.strip()

    return last, first, zdb, name_email


def extract_email(address_lines, name_email):
    """Extract email from address lines or name line. Return (email, remaining_lines)."""
    if name_email:
        email = name_email
    else:
        email = ''

    remaining = []
    for i, line in enumerate(address_lines):
        if EMAIL_RE.match(line):
            if not email:
                email = line.strip()
            # skip duplicate email lines
        else:
            remaining.append(line)

    return email, remaining


def is_country_line(line):
    """Check if a line is a standalone country name. Return canonical name or None."""
    cleaned = line.strip().rstrip('.').strip()
    # handle "Spain including Canary Islands..." → "Spain"
    for country_lower, canonical in COUNTRIES.items():
        if cleaned.lower() == country_lower:
            return canonical
        if cleaned.lower().startswith(country_lower + ' '):
            return canonical
    return None


def looks_like_postal(text):
    """Check if text looks like a postal code."""
    text = text.strip().rstrip('.')
    if not text:
        return False
    if US_ZIP_RE.match(text):
        return True
    if UK_POST_RE.match(text):
        return True
    if POLISH_POST_RE.match(text):
        return True
    if DUTCH_POST_RE.match(text):
        return True
    if CANADA_POST_RE.match(text):
        return True
    if JAPAN_POST_RE.match(text):
        return True
    if SWEDEN_POST_RE.match(text):
        return True
    if INTL_POST_RE.match(text):
        return True
    return False


def extract_address_components(lines):  # noqa: C901
    """Extract city, state, postal_code, country from address lines.

    Returns (city, state, postal_code, country, unmatched_text).
    """
    city = state = postal = country = ''
    if not lines:
        return city, state, postal, country, ''

    consumed = set()
    work = list(lines)

    # find last non-empty line index
    last_idx = len(work) - 1
    while last_idx >= 0 and not work[last_idx].strip():
        last_idx -= 1
    if last_idx < 0:
        return city, state, postal, country, ''

    # Step 1: check last line for country
    country_match = is_country_line(work[last_idx])
    if country_match:
        country = country_match
        consumed.add(last_idx)
        last_idx -= 1
        while last_idx >= 0 and not work[last_idx].strip():
            last_idx -= 1

    if last_idx < 0:
        unmatched = _join_unmatched(work, consumed)
        return city, state, postal, country, unmatched

    # Step 1b: check if last line ends with ", Country"
    # e.g. "Helsinki, Finland" or "...Nijmegen, The Netherlands"
    if not country and last_idx >= 0:
        line = work[last_idx].strip().rstrip('.')
        # greedy match: split on LAST comma
        m = re.match(r'^(.+)\s*,\s*(.+)$', line)
        if m:
            country_match2 = is_country_line(m.group(2).strip())
            if country_match2:
                country = country_match2
                remainder = m.group(1).strip()
                # check if remainder ends with a postal code
                m_postal = re.search(
                    r',\s*(\S+(?:\s+\S+)?)\s*$', remainder
                )
                if m_postal and looks_like_postal(m_postal.group(1)):
                    postal = m_postal.group(1)
                    remainder = remainder[:m_postal.start()].strip()
                # last comma-separated token is likely city
                parts = remainder.rsplit(',', 1)
                if len(parts) > 1:
                    city = parts[-1].strip()
                else:
                    city = remainder.strip()
                consumed.add(last_idx)
                last_idx -= 1
                while last_idx >= 0 and not work[last_idx].strip():
                    last_idx -= 1

    if last_idx < 0:
        unmatched = _join_unmatched(work, consumed)
        return city, state, postal, country, unmatched

    # Step 2: check for standalone postal code line
    line = work[last_idx].strip().rstrip('.')
    if looks_like_postal(line) and len(line) < 15:
        postal = line
        consumed.add(last_idx)
        last_idx -= 1
        while last_idx >= 0 and not work[last_idx].strip():
            last_idx -= 1

    if last_idx < 0:
        unmatched = _join_unmatched(work, consumed)
        return city, state, postal, country, unmatched

    # Step 3: city/state patterns on current last line
    line = work[last_idx].strip().rstrip('.')

    # Pattern: "City, ST" or "City , ST" (US state, no zip on same line)
    m = re.match(r'^(.+?)\s*,\s*([A-Z]{2})\s*$', line)
    if m and m.group(2) in STATE_CODES:
        city = m.group(1).strip()
        state = m.group(2)
        consumed.add(last_idx)
    else:
        # Pattern: "City, ST ZIP" or "City, ST  ZIP" or "City, ST PostalCode"
        m = re.match(
            r'^(.+?)\s*,\s*([A-Z]{2})\s+(.+?)\s*\.?\s*$', line
        )
        if m and m.group(2) in STATE_CODES and looks_like_postal(m.group(3)):
            city = m.group(1).strip()
            state = m.group(2)
            if not postal:
                postal = m.group(3)
            consumed.add(last_idx)
        else:
            # Pattern: "City ST ZIP" (no comma)
            m = re.match(
                r'^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*\.?\s*$', line
            )
            if m and m.group(2) in STATE_CODES:
                city = m.group(1).strip()
                state = m.group(2)
                if not postal:
                    postal = m.group(3)
                consumed.add(last_idx)
            else:
                # Pattern: "City, PostalCode" (international)
                # Try multi-word postal first (UK: "CB2 3DY", Dutch: "3000 CA")
                m = re.match(r'^(.+?)\s*,\s*(.+)$', line)
                if m and looks_like_postal(m.group(2).strip()):
                    city = m.group(1).strip()
                    if not postal:
                        postal = m.group(2).strip().rstrip('.')
                    consumed.add(last_idx)
                else:
                    # Pattern: "PostalCode City" or "PREFIX-PostalCode City"
                    # e.g. "02-109 Warsaw", "SE-41345 Gothenburg",
                    # "60438 Frankfurt"
                    m = re.match(
                        r'^(\d{2}-\d{3})\s+(.+)$', line
                    )
                    if not m:
                        m = re.match(
                            r'^([A-Z]{0,3}[\s-]?\d{3,6})\s+(.+)$', line,
                            re.IGNORECASE
                        )
                    if m and looks_like_postal(m.group(1)):
                        if not postal:
                            postal = m.group(1).strip()
                        city = m.group(2).strip()
                        consumed.add(last_idx)
                    else:
                        # Pattern: "City, Region, PostalCode"
                        m = re.match(
                            r'^(.+?)\s*,\s*(.+?)\s*,\s*(.+)\s*$', line
                        )
                        if m and looks_like_postal(m.group(3).strip()):
                            city = m.group(1).strip()
                            if not postal:
                                postal = m.group(3).strip().rstrip('.')
                            consumed.add(last_idx)
                        elif m and looks_like_postal(m.group(2).strip()):
                            # Pattern: "Street, PostalCode, City"
                            # e.g. "Einsteinweg 55, 2333 CC, Leiden"
                            city = m.group(3).strip()
                            if not postal:
                                postal = m.group(2).strip()
                            consumed.add(last_idx)

    # Step 3b: if no city yet, scan lines for "..., PostalCode City"
    # or "City PostalCode" embedded at end of a line
    if not city:
        for scan_idx in reversed(range(len(work))):
            if scan_idx in consumed:
                continue
            line = work[scan_idx].strip().rstrip('.')
            # "..., PostalCode City" at end (e.g., "Sognsvannsveien 9, 0317 Oslo")
            m = re.search(
                r',\s*(\d{3,6})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*$',
                line
            )
            if m and looks_like_postal(m.group(1)):
                if not postal:
                    postal = m.group(1)
                city = m.group(2).strip()
                # keep the street part in unmatched
                remainder = line[:m.start()].strip()
                work[scan_idx] = remainder
                break
            # "City PostalCode" at end (e.g., "Manchester M13 9PT",
            # "Shanghai 200031", "Hiroshima, 734-8553")
            m = re.search(
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,?\s+'
                r'([A-Z]?\d[\dA-Z]*(?:[\s-]\d[A-Z]{2})?)\s*$',
                line
            )
            if m and looks_like_postal(m.group(2)):
                city = m.group(1).strip()
                if not postal:
                    postal = m.group(2).strip()
                remainder = line[:m.start()].strip()
                work[scan_idx] = remainder if remainder else work[scan_idx]
                if not remainder:
                    consumed.add(scan_idx)
                break

    # Step 4: scan all unconsumed lines for embedded country at end of line
    if not country:
        for scan_idx in reversed(range(len(work))):
            if scan_idx in consumed:
                continue
            line = work[scan_idx].strip().rstrip('.')
            for country_lower, canonical in COUNTRIES.items():
                # "..., Country" or "...PostalCode Country" at end of line
                pattern = r'[,\s]\s*' + re.escape(canonical) + r'\s*\.?\s*$'
                m_country = re.search(pattern, line, re.IGNORECASE)
                if m_country:
                    country = canonical
                    # include the matched separator char in what we strip
                    remainder = line[:m_country.start() + 1].strip()
                    remainder = remainder.rstrip(',').strip()
                    # try to extract postal from remainder
                    m_postal = re.search(
                        r'[,\s]\s*(\d{2,3}[\s-]?\d{3,6}(?:[-\s]?\w{0,5})?)'
                        r'\s*$',
                        remainder
                    )
                    if m_postal and looks_like_postal(m_postal.group(1)):
                        if not postal:
                            postal = m_postal.group(1).strip()
                        remainder = remainder[:m_postal.start()].strip()
                        remainder = remainder.rstrip(',').strip()
                    if remainder and not city:
                        parts = remainder.rsplit(',', 1)
                        if len(parts) > 1:
                            city = parts[-1].strip()
                        else:
                            city = remainder.strip()
                    consumed.add(scan_idx)
                    break
            if country:
                break

    # Step 4b: check for "(Country)" pattern in any line
    if not country:
        for scan_idx in range(len(work)):
            if scan_idx in consumed:
                continue
            line = work[scan_idx].strip()
            m = re.search(r'\((\w[\w\s]*)\)\s*$', line)
            if m:
                country_match_paren = is_country_line(m.group(1).strip())
                if country_match_paren:
                    country = country_match_paren
                    remainder = line[:m.start()].strip().rstrip(',').strip()
                    # try postal before the parens
                    m_p = re.search(r'(\d{3,6})\s*$', remainder)
                    if m_p and looks_like_postal(m_p.group(1)):
                        if not postal:
                            postal = m_p.group(1)
                        remainder = remainder[:m_p.start()].strip()
                        remainder = remainder.rstrip(',').strip()
                    if remainder and not city:
                        parts = remainder.rsplit(',', 1)
                        city = parts[-1].strip() if len(parts) > 1 else ''
                    consumed.add(scan_idx)
                    break

    # Step 5: country-less postal extraction
    # Scan for postal code patterns even without a country anchor
    if not city and not postal:
        for scan_idx in reversed(range(len(work))):
            if scan_idx in consumed:
                continue
            line = work[scan_idx].strip().rstrip('.')

            # Japanese postal NNN-NNNN at end: "Osaka 565-0871"
            m = re.search(
                r'[,\s]\s*([A-Z][\w]*(?:\s+[A-Z][\w]*)*)'
                r'\s+(\d{3}-\d{4})\s*$',
                line
            )
            if m and JAPAN_POST_RE.match(m.group(2)):
                city = m.group(1).strip()
                postal = m.group(2)
                if not country:
                    country = 'Japan'
                remainder = line[:m.start() + 1].strip().rstrip(',')
                work[scan_idx] = remainder if remainder else work[scan_idx]
                if not remainder:
                    consumed.add(scan_idx)
                break

            # Swedish postal NNN NN + City: "751 24 Uppsala"
            m = re.search(
                r'(\d{3}\s\d{2})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*$',
                line
            )
            if m and SWEDEN_POST_RE.match(m.group(1)):
                postal = m.group(1)
                city = m.group(2).strip()
                remainder = line[:m.start()].strip().rstrip(',')
                work[scan_idx] = remainder if remainder else work[scan_idx]
                if not remainder:
                    consumed.add(scan_idx)
                break

            # French/generic: "NNNNN City" at end: "75015 Paris"
            m = re.search(
                r'(\d{4,6})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*$',
                line
            )
            if m and looks_like_postal(m.group(1)):
                postal = m.group(1)
                city = m.group(2).strip()
                remainder = line[:m.start()].strip().rstrip(',')
                work[scan_idx] = remainder if remainder else work[scan_idx]
                if not remainder:
                    consumed.add(scan_idx)
                break

    # Indian postal: "New Delhi - 110 025" or "New Delhi 110025"
    if not postal:
        for scan_idx in reversed(range(len(work))):
            if scan_idx in consumed:
                continue
            line = work[scan_idx].strip()
            m = re.search(
                r'([A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s*[-,]\s*'
                r'(\d{3}\s?\d{3})\s*$',
                line
            )
            if m:
                city = m.group(1).strip()
                postal = m.group(2)
                remainder = line[:m.start()].strip().rstrip(',')
                work[scan_idx] = remainder if remainder else work[scan_idx]
                if not remainder:
                    consumed.add(scan_idx)
                break

    # Handle malformed Canadian postal: "ONK1H 8L1" → "ON" + "K1H 8L1"
    if not postal:
        for scan_idx in reversed(range(len(work))):
            if scan_idx in consumed:
                continue
            line = work[scan_idx].strip()
            m = re.search(
                r'([A-Z]{2})([A-Z]\d[A-Z]\s?\d[A-Z]\d)\s*$', line
            )
            if m and m.group(1) in STATE_CODES:
                state = m.group(1)
                raw_postal = m.group(2)
                if len(raw_postal) == 6:
                    postal = raw_postal[:3] + ' ' + raw_postal[3:]
                else:
                    postal = raw_postal
                remainder = line[:m.start()].strip().rstrip(',')
                if remainder:
                    parts = remainder.rsplit(',', 1)
                    if len(parts) > 1:
                        city = parts[-1].strip()
                    work[scan_idx] = remainder
                else:
                    consumed.add(scan_idx)
                break

    # Brazilian "City, RS" where RS is a Brazilian state
    if not city and not country:
        for scan_idx in reversed(range(len(work))):
            if scan_idx in consumed:
                continue
            line = work[scan_idx].strip().rstrip('.')
            m = re.match(r'^(.+)\s*,\s*([A-Z]{2})\s*$', line)
            if m and m.group(2) in BRAZILIAN_STATES:
                city = m.group(1).strip()
                state = m.group(2)
                country = 'Brazil'
                consumed.add(scan_idx)
                break

    # Step 6: city → country inference
    # If we have a city but no country, look it up
    if city and not country:
        city_lower = city.lower()
        if city_lower in CITY_TO_COUNTRY:
            country = CITY_TO_COUNTRY[city_lower]

    # If we still have nothing, scan all text for known city names
    if not city and not country:
        all_text = ' '.join(
            work[i].strip().lower() for i in range(len(work))
            if i not in consumed
        )
        for city_name, ctry in CITY_TO_COUNTRY.items():
            if city_name in all_text:
                city = city_name.title()
                country = ctry
                break

    unmatched = _join_unmatched(work, consumed)
    return city, state, postal, country, unmatched


def _join_unmatched(lines, consumed):
    """Join lines not in consumed set with ' | ' separator."""
    parts = []
    for i, line in enumerate(lines):
        if i not in consumed and line.strip():
            parts.append(line.strip())
    return ' | '.join(parts)


def main():
    entries = parse_entries(INPUT_FILE)
    results = []
    no_address_rows = []
    unparsed_rows = []
    matched_count = 0
    unparsed_count = 0
    no_address_count = 0

    for name_portion, address_lines in entries:
        last, first, zdb, name_email = extract_name(name_portion)
        email, remaining = extract_email(address_lines, name_email)
        city, state_val, postal, country, unmatched = extract_address_components(
            remaining
        )

        has_match = any([city, state_val, postal, country])
        has_address = bool(remaining)

        if has_match:
            matched_count += 1
        elif has_address:
            unparsed_count += 1
        else:
            no_address_count += 1

        row = {
            'last': last,
            'first': first,
            'zdb': zdb,
            'email': email,
            'address': unmatched,
            'city': city,
            'state': state_val,
            'postal_code': postal,
            'country': country,
            'matched': 'Y' if has_match else 'N',
        }
        results.append(row)
        if not has_match:
            if has_address:
                unparsed_rows.append(row)
            else:
                no_address_rows.append(row)

    # Write main TSV
    headers = [
        'last', 'first', 'zdb', 'email', 'address',
        'city', 'state', 'postal_code', 'country', 'matched',
    ]
    short_headers = ['last', 'first', 'zdb', 'email', 'address']

    def write_tsv(filepath, hdrs, rows):
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\t'.join(hdrs) + '\n')
            for row in rows:
                f.write('\t'.join(row[h] for h in hdrs) + '\n')

    write_tsv(OUTPUT_FILE, headers, results)
    write_tsv(NO_ADDRESS_FILE, short_headers, no_address_rows)
    write_tsv(NO_MATCH_ADDRESS_FILE, short_headers, unparsed_rows)

    # Summary
    total = len(results)
    summary_lines = [
        f"Total persons: {total}",
        f"Matched (some address data extracted): {matched_count}",
        f"Unparsed (has address, nothing extracted): {unparsed_count}",
        f"No address (no address data at all): {no_address_count}",
    ]
    summary_text = '\n'.join(summary_lines)
    print(summary_text)

    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write(summary_text + '\n')

    print(f"\nOutput written to {OUTPUT_FILE}")
    print(f"No-address entries written to {NO_ADDRESS_FILE}")
    print(f"Unparsed entries written to {NO_MATCH_ADDRESS_FILE}")
    print(f"Summary written to {SUMMARY_FILE}")


if __name__ == '__main__':
    main()
