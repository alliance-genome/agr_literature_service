import re

from calendar import monthrange
from datetime import datetime


def parse_date(date_string, validate_flag=False):
    # if validate_flag, use datetime.strptime against the expected format to validate it converts to a date
    # processing 905109 dates takes 67 seconds with validation versus 51 seconds without validation
    # parsers in in the order of most frequent data match to lower, avoid reordering without good reason
    date_string = date_string.replace("/", "-")  # standardize notation
    date_string = date_string.replace(" - ", "-")  # remove padding
    date_string = date_string.rstrip('-')  # remove trailing hyphen
    date_string = date_string.rstrip('.')  # remove trailing period
    date_string = date_string.rstrip(',')  # remove trailing comma
    date_string = date_string.replace(".-", "-")  # remove period before hyphen
    result = parse_numeric_date(date_string)
    if not result:
        result = parse_text_date(date_string)
    if not result:
        result = parse_hardcoded_pubmed_exceptions(date_string)
    if not result or validate_flag is False:
        return result, None
    else:
        try:
            datetime.strptime(result[0], '%Y-%m-%d').date()
            datetime.strptime(result[1], '%Y-%m-%d').date()
            return result, None
        except ValueError:
            return False, f"date_string to {result[0]} {result[1]} is not a date"


def parse_numeric_date(date_string):
    result = parse_just_date(date_string)
    if not result:
        result = parse_just_year(date_string)
    if not result:
        result = parse_dot_date(date_string)
    if not result:
        result = parse_year_dash_year(date_string)
    if not result:
        result = parse_year_dot_month(date_string)
    if not result:
        result = parse_year_dot_or_dash_month(date_string)
    if not result:
        result = parse_year_space_month_dash_day(date_string)
    if not result:
        result = parse_date_timezone(date_string)
    return result


def parse_text_date(date_string):
    result = parse_year_space_mon_dash_mon(date_string)
    if not result:
        result = parse_year_space_mon_space_day_dash_day(date_string)
    if not result:
        result = parse_year_space_mon_space_day_dash_mon_space_day(date_string)
    if not result:
        result = parse_year_space_mon(date_string)
    if not result:
        result = parse_year_space_mon_space_day(date_string)
    if not result:
        result = parse_year_space_mon_dash_mon_space_day(date_string)
    if not result:
        result = parse_year_space_mon_space_day_dash_year_space_mon_space_day(date_string)
    if not result:
        result = parse_year_space_mon_dash_year_space_mon(date_string)
    if not result:
        result = parse_year_space_season(date_string)
    if not result:
        result = parse_year_space_mon_space_mon(date_string)
    if not result:
        result = parse_year_space_mon_dash_mon_space_year(date_string)
    return result


def parse_hardcoded_pubmed_exceptions(date_string):
    # manual hardcoded things for data that comes from PubMed
    hardcoded_dict = {
        '1986-1987 Jan' : {'start': '1986-01-01', 'end': '1986-12-31'},
        '2020 March-April' : {'start': '2020-03-01', 'end': '2020-04-30'},
        '1992 Aug 15-Sep' : {'start': '1992-08-15', 'end': '1992-09-30'},
        '2016 Supplement 1' : {'start': '2016-01-01', 'end': '2016-12-31'},
        '2022 May-June' : {'start': '2022-05-01', 'end': '2022-06-30'}
    }
    if date_string not in hardcoded_dict:
        return False
    else:
        return f"{hardcoded_dict[date_string]['start']}", f"{hardcoded_dict[date_string]['end']}"


def parse_year_space_mon_space_mon(date_string):
    # year space three letter month abbreviation space three letter month abbreviation
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3}) ([A-Za-z]{3})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month1 = month_name_to_number_string(re_output.group(2))
        month2 = month_name_to_number_string(re_output.group(3))
        days = monthrange(int(year), int(month2))[1]
        return f"{year}-{month1}-01", f"{year}-{month2}-{days}"
    return False


def parse_year_space_mon_dash_mon_space_year(date_string):
    # year space three letter month abbreviation dash three letter month abbreviation space year
    # e.g. 2017 Jan-Feb 2017
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3})-([A-Za-z]{3}) (\d{4})$", date_string)
    if re_output is not None:
        year1 = re_output.group(1)
        month1 = month_name_to_number_string(re_output.group(2))
        year2 = re_output.group(4)
        month2 = month_name_to_number_string(re_output.group(3))
        days = monthrange(int(year2), int(month2))[1]
        return f"{year1}-{month1}-01", f"{year2}-{month2}-{days}"
    return False


def parse_year_space_season(date_string):
    # year space season range
    # e.g. 1996 Autumn-Winter  1997 Fall-Winter  2000 Spring-Summer
    re_output = re.search(r"^(\d{4}) ([A-Za-z\-]*)$", date_string)
    if re_output is not None:
        season_range_dict = {
            'Summer-Autumn': {'start': '06-01', 'end': '10-31'},
            'Autumn-Winter': {'start': '09-01', 'end': '12-31'},
            'Fall-Winter': {'start': '09-01', 'end': '12-31'},
            'Spring-Summer': {'start': '03-01', 'end': '08-31'}
        }
        year = re_output.group(1)
        season_range = re_output.group(2)
        if season_range not in season_range_dict:
            return False
        else:
            return f"{year}-{season_range_dict[season_range]['start']}", f"{year}-{season_range_dict[season_range]['end']}"
    return False


def parse_year_space_mon_dash_year_space_mon(date_string):
    # year space three letter month abbreviation dash year space three letter month abbreviation
    # e.g. 1988 Dec-1989 Feb
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3})-(\d{4}) ([A-Za-z]{3})$", date_string)
    if re_output is not None:
        year1 = re_output.group(1)
        month1 = month_name_to_number_string(re_output.group(2))
        year2 = re_output.group(3)
        month2 = month_name_to_number_string(re_output.group(4))
        days = monthrange(int(year2), int(month2))[1]
        return f"{year1}-{month1}-01", f"{year2}-{month2}-{days}"
    return False


def parse_year_space_mon_space_day_dash_year_space_mon_space_day(date_string):
    # year space three letter month abbreviation space day dash year space three letter month abbreviation space day
    # e.g. 1985 Dec 19-1986 Jan 1
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3}) (\d{1,2})-(\d{4}) ([A-Za-z]{3}) (\d{1,2})$", date_string)
    if re_output is not None:
        year1 = re_output.group(1)
        month1 = month_name_to_number_string(re_output.group(2))
        day1 = re_output.group(3)
        year2 = re_output.group(4)
        month2 = month_name_to_number_string(re_output.group(5))
        day2 = re_output.group(6)
        return f"{year1}-{month1}-{day1}", f"{year2}-{month2}-{day2}"
    return False


def parse_year_space_mon_dash_mon_space_day(date_string):
    # year space three letter month abbreviation dash three letter month abbreviation space day
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3})-([A-Za-z]{3}) (\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month1 = month_name_to_number_string(re_output.group(2))
        month2 = month_name_to_number_string(re_output.group(3))
        day = re_output.group(4)
        return f"{year}-{month1}-{day}", f"{year}-{month2}-{day}"
    return False


def parse_year_space_mon_space_day(date_string):
    # year space three letter month abbreviation space day dash three letter month abbreviation space day
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3}) (\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month = month_name_to_number_string(re_output.group(2))
        day = re_output.group(3)
        return f"{year}-{month}-{day}", f"{year}-{month}-{day}"
    return False


def parse_year_space_mon(date_string):
    # year space three letter month abbreviation
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month = month_name_to_number_string(re_output.group(2))
        days = monthrange(int(year), int(month))[1]
        return f"{year}-{month}-01", f"{year}-{month}-{days}"
    return False


def parse_year_space_mon_space_day_dash_mon_space_day(date_string):
    # year space three letter month abbreviation space day dash three letter month abbreviation space day
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3}) (\d{1,2})-([A-Za-z]{3}) (\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month1 = month_name_to_number_string(re_output.group(2))
        day1 = re_output.group(3)
        month2 = month_name_to_number_string(re_output.group(4))
        day2 = re_output.group(5)
        return f"{year}-{month1}-{day1}", f"{year}-{month2}-{day2}"
    return False


def parse_year_space_mon_space_day_dash_day(date_string):
    # year space three letter month abbreviation space day dash day
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3}) (\d{1,2})-(\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month = month_name_to_number_string(re_output.group(2))
        day1 = re_output.group(3)
        day2 = re_output.group(4)
        return f"{year}-{month}-{day1}", f"{year}-{month}-{day2}"
    return False


def parse_year_space_mon_dash_mon(date_string):
    # year space three letter month abbreviation dash letter month abbreviation
    re_output = re.search(r"^(\d{4}) ([A-Za-z]{3})-([A-Za-z]{3})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month1 = month_name_to_number_string(re_output.group(2))
        month2 = month_name_to_number_string(re_output.group(3))
        days = monthrange(int(year), int(month2))[1]
        return f"{year}-{month1}-01", f"{year}-{month2}-{days}"
    return False


def parse_date_timezone(date_string):
    just_year_re_output = re.search(r"^(\d{4})-(\d{2})-(\d{2})T00", date_string)
    if just_year_re_output is not None:
        year = just_year_re_output.group(1)
        month = just_year_re_output.group(2)
        day = just_year_re_output.group(3)
        return f"{year}-{month}-{day}", f"{year}-{month}-{day}"
    return False


def parse_year_space_month_dash_day(date_string):
    # PMID:27566080 has this in the MedlineDate
    re_output = re.search(r"^(\d{4}) (\d{1,2})-(\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month = re_output.group(2)
        day = re_output.group(3)
        return f"{year}-{month}-{day}", f"{year}-{month}-{day}"
    return False


def parse_year_dot_or_dash_month(date_string):
    re_output = re.search(r"^(\d{4})[-\.](\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month = re_output.group(2)
        if int(month) > 12:
            return False
        days = monthrange(int(year), int(month))[1]
        return f"{year}-{month}-01", f"{year}-{month}-{days}"
    return False


def parse_year_dot_month(date_string):
    re_output = re.search(r"^(\d{4})\.(\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month = re_output.group(2)
        days = monthrange(int(year), int(month))[1]
        return f"{year}-{month}-01", f"{year}-{month}-{days}"
    return False


def parse_year_dash_year(date_string):
    re_output = re.search(r"^(\d{4})-(\d{4})$", date_string)
    if re_output is not None:
        year1 = re_output.group(1)
        year2 = re_output.group(2)
        return f"{year1}-01-01", f"{year2}-12-31"
    return False


def parse_dot_date(date_string):
    re_output = re.search(r"^(\d{4})\.(\d{1,2})\.(\d{1,2})$", date_string)
    if re_output is not None:
        year = re_output.group(1)
        month = re_output.group(2)
        day = re_output.group(3)
        return f"{year}-{month}-{day}", f"{year}-{month}-{day}"
    return False


def parse_just_year(date_string):
    just_year_re_output = re.search(r"^(\d{4})$", date_string)
    if just_year_re_output is not None:
        year = just_year_re_output.group(1)
        return f"{year}-01-01", f"{year}-12-31"
    return False


def parse_just_date(date_string):
    just_year_re_output = re.search(r"^(\d{4})-(\d{2})-(\d{2})$", date_string)
    if just_year_re_output is not None:
        year = just_year_re_output.group(1)
        month = just_year_re_output.group(2)
        day = just_year_re_output.group(3)
        return f"{year}-{month}-{day}", f"{year}-{month}-{day}"
    return False


def month_name_to_number_string(string):
    """

    :param string:
    :return:
    """

    m = {
        'jan': '01',
        'feb': '02',
        'mar': '03',
        'nar': '03',  # for MGI typo MGI:2155139
        'apr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'aug': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dec': '12'}
    s = string.strip()[:3].lower()

    if s not in m:
        return False
    else:
        try:
            out = m[s]
            return out
        except ValueError:
            raise ValueError(string + ' is not a month')
