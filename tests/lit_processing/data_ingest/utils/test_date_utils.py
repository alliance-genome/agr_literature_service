# import json
# import os
# from os import environ

from agr_literature_service.lit_processing.data_ingest.utils.date_utils import \
    parse_numeric_date, parse_text_date, parse_hardcoded_pubmed_exceptions, \
    parse_year_space_mon_space_mon, parse_year_space_mon_dash_mon_space_year, \
    parse_year_space_season, parse_year_space_mon_dash_year_space_mon, \
    parse_year_space_mon_space_day_dash_year_space_mon_space_day, \
    parse_year_space_mon_dash_mon_space_day, parse_year_space_mon_space_day, \
    parse_year_space_mon, parse_year_space_mon_space_day_dash_mon_space_day, \
    parse_year_space_mon_space_day_dash_day, parse_year_space_mon_dash_mon, \
    parse_date_timezone, parse_year_space_month_dash_day, \
    parse_year_dot_or_dash_month, parse_year_dash_year, parse_dot_date, \
    parse_just_year, parse_just_date, parse_date, validate_date_format, \
    month_name_to_number_string


class TestDateUtils:

    def test_parse_just_date(self):
        assert parse_just_date('1998-11-01') == ('1998-11-01', '1998-11-01')

    def test_parse_just_year(self):
        assert parse_just_year('2016') == ('2016-01-01', '2016-12-31')

    def test_parse_dot_date(self):
        assert parse_dot_date('2000.11.30') == ('2000-11-30', '2000-11-30')

    def test_parse_year_dash_year(self):
        assert parse_year_dash_year('1932-1933') == ('1932-01-01', '1933-12-31')

    def test_parse_year_dot_or_dash_month(self):
        assert parse_year_dot_or_dash_month('2003.12') == ('2003-12-01', '2003-12-31')
        assert parse_year_dot_or_dash_month('2003-10') == ('2003-10-01', '2003-10-31')

    def test_parse_year_space_month_dash_day(self):
        assert parse_year_space_month_dash_day('2016 09-10') == ('2016-09-10', '2016-09-10')

    def test_parse_date_timezone(self):
        assert parse_date_timezone('2006-02-01T00:00:00.000-06:00') == ('2006-02-01', '2006-02-01')

    def test_parse_year_space_mon_dash_mon(self):
        assert parse_year_space_mon_dash_mon('2021 Jan-Dec') == ('2021-01-01', '2021-12-31')

    def test_parse_year_space_mon_space_day_dash_day(self):
        assert parse_year_space_mon_space_day_dash_day('1999 Dec 16-30') == ('1999-12-16', '1999-12-30')

    def test_parse_year_space_mon_space_day_dash_mon_space_day(self):
        assert parse_year_space_mon_space_day_dash_mon_space_day('1999 Jan 15-Feb 1') == ('1999-01-15', '1999-02-1')

    def test_parse_year_space_mon(self):
        assert parse_year_space_mon('1965 Jun') == ('1965-06-01', '1965-06-30')

    def test_parse_year_space_mon_space_day(self):
        assert parse_year_space_mon_space_day('1991 Feb 22') == ('1991-02-22', '1991-02-22')

    def test_parse_year_space_mon_dash_mon_space_day(self):
        assert parse_year_space_mon_dash_mon_space_day('2002 Feb-Mar 1') == ('2002-02-1', '2002-03-1')

    def test_parse_year_space_mon_space_day_dash_year_space_mon_space_day(self):
        assert parse_year_space_mon_space_day_dash_year_space_mon_space_day('1985 Dec 19-1986 Jan 1') == ('1985-12-19', '1986-01-1')

    def test_parse_year_space_mon_dash_year_space_mon(self):
        assert parse_year_space_mon_dash_year_space_mon('1988 Dec-1989 Feb') == ('1988-12-01', '1989-02-28')

    def test_parse_year_space_season(self):
        assert parse_year_space_season('1988 Summer-Autumn') == ('1988-06-01', '1988-10-31')
        assert parse_year_space_season('1996 Autumn-Winter') == ('1996-09-01', '1996-12-31')
        assert parse_year_space_season('1997 Fall-Winter') == ('1997-09-01', '1997-12-31')
        assert parse_year_space_season('1998 Spring-Summer') == ('1998-03-01', '1998-08-31')

    def test_parse_year_space_mon_dash_mon_space_year(self):
        assert parse_year_space_mon_dash_mon_space_year('2017 Jan-Feb 2017') == ('2017-01-01', '2017-02-28')

    def test_parse_year_space_mon_space_mon(self):
        assert parse_year_space_mon_space_mon('1999 Nov Dec') == ('1999-11-01', '1999-12-31')

    def test_parse_hardcoded_pubmed_exceptions(self):
        assert parse_hardcoded_pubmed_exceptions('1986-1987 Jan') == ('1986-01-01', '1986-12-31')
        assert parse_hardcoded_pubmed_exceptions('2020 March-April') == ('2020-03-01', '2020-04-30')
        assert parse_hardcoded_pubmed_exceptions('1992 Aug 15-Sep') == ('1992-08-15', '1992-09-30')
        assert parse_hardcoded_pubmed_exceptions('2016 Supplement 1') == ('2016-01-01', '2016-12-31')
        assert parse_hardcoded_pubmed_exceptions('2022 May-June') == ('2022-05-01', '2022-06-30')

    def test_parse_numeric_date(self):
        assert parse_numeric_date('1998-11-01') == ('1998-11-01', '1998-11-01')
        assert parse_numeric_date('2016') == ('2016-01-01', '2016-12-31')
        assert parse_numeric_date('2000.11.30') == ('2000-11-30', '2000-11-30')
        assert parse_numeric_date('1932-1933') == ('1932-01-01', '1933-12-31')
        assert parse_numeric_date('2003.12') == ('2003-12-01', '2003-12-31')
        assert parse_numeric_date('2003-10') == ('2003-10-01', '2003-10-31')
        assert parse_numeric_date('2016 09-10') == ('2016-09-10', '2016-09-10')
        assert parse_numeric_date('2006-02-01T00:00:00.000-06:00') == ('2006-02-01', '2006-02-01')

    def test_parse_text_date(self):
        assert parse_text_date('2021 Jan-Dec') == ('2021-01-01', '2021-12-31')
        assert parse_text_date('1999 Dec 16-30') == ('1999-12-16', '1999-12-30')
        assert parse_text_date('1999 Jan 15-Feb 1') == ('1999-01-15', '1999-02-1')
        assert parse_text_date('1965 Jun') == ('1965-06-01', '1965-06-30')
        assert parse_text_date('1991 Feb 22') == ('1991-02-22', '1991-02-22')
        assert parse_text_date('2002 Feb-Mar 1') == ('2002-02-1', '2002-03-1')
        assert parse_text_date('1985 Dec 19-1986 Jan 1') == ('1985-12-19', '1986-01-1')
        assert parse_text_date('1988 Dec-1989 Feb') == ('1988-12-01', '1989-02-28')
        assert parse_text_date('1988 Summer-Autumn') == ('1988-06-01', '1988-10-31')
        assert parse_text_date('1996 Autumn-Winter') == ('1996-09-01', '1996-12-31')
        assert parse_text_date('1997 Fall-Winter') == ('1997-09-01', '1997-12-31')
        assert parse_text_date('1998 Spring-Summer') == ('1998-03-01', '1998-08-31')
        assert parse_text_date('2017 Jan-Feb 2017') == ('2017-01-01', '2017-02-28')
        assert parse_text_date('1999 Nov Dec') == ('1999-11-01', '1999-12-31')

    def test_parse_date_valid(self):
        assert parse_date('1998-11-01', False) == (('1998-11-01', '1998-11-01'), None)
        assert parse_date('2016', False) == (('2016-01-01', '2016-12-31'), None)
        assert parse_date('2000.11.30', False) == (('2000-11-30', '2000-11-30'), None)
        assert parse_date('1932-1933', False) == (('1932-01-01', '1933-12-31'), None)
        assert parse_date('2003.12', False) == (('2003-12-01', '2003-12-31'), None)
        assert parse_date('2003-10', False) == (('2003-10-01', '2003-10-31'), None)
        assert parse_date('2016 09-10', False) == (('2016-09-10', '2016-09-10'), None)
        assert parse_date('2006-02-01T00:00:00.000-06:00', False) == (('2006-02-01', '2006-02-01'), None)
        assert parse_date('2021 Jan-Dec', False) == (('2021-01-01', '2021-12-31'), None)
        assert parse_date('1999 Dec 16-30', False) == (('1999-12-16', '1999-12-30'), None)
        assert parse_date('1999 Jan 15-Feb 1', False) == (('1999-01-15', '1999-02-1'), None)
        assert parse_date('1965 Jun', False) == (('1965-06-01', '1965-06-30'), None)
        assert parse_date('1991 Feb 22', False) == (('1991-02-22', '1991-02-22'), None)
        assert parse_date('2002 Feb-Mar 1', False) == (('2002-02-1', '2002-03-1'), None)
        assert parse_date('1985 Dec 19-1986 Jan 1', False) == (('1985-12-19', '1986-01-1'), None)
        assert parse_date('1988 Dec-1989 Feb', False) == (('1988-12-01', '1989-02-28'), None)
        assert parse_date('1988 Summer-Autumn', False) == (('1988-06-01', '1988-10-31'), None)
        assert parse_date('1996 Autumn-Winter', False) == (('1996-09-01', '1996-12-31'), None)
        assert parse_date('1997 Fall-Winter', False) == (('1997-09-01', '1997-12-31'), None)
        assert parse_date('1998 Spring-Summer', False) == (('1998-03-01', '1998-08-31'), None)
        assert parse_date('2017 Jan-Feb 2017', False) == (('2017-01-01', '2017-02-28'), None)
        assert parse_date('1999 Nov Dec', False) == (('1999-11-01', '1999-12-31'), None)
        assert parse_date('1986-1987 Jan', False) == (('1986-01-01', '1986-12-31'), None)
        assert parse_date('2020 March-April', False) == (('2020-03-01', '2020-04-30'), None)
        assert parse_date('1992 Aug 15-Sep', False) == (('1992-08-15', '1992-09-30'), None)
        assert parse_date('2016 Supplement 1', False) == (('2016-01-01', '2016-12-31'), None)
        assert parse_date('2022 May-June', False) == (('2022-05-01', '2022-06-30'), None)

    def test_parse_date_valid_validation_success(self):
        assert parse_date('1998-11-01', True) == (('1998-11-01', '1998-11-01'), None)
        assert parse_date('2016', True) == (('2016-01-01', '2016-12-31'), None)

    def test_validate_date_format_success(self):
        assert validate_date_format('2007-01-01', ('2007-01-01', '2007-01-01')) == \
            (('2007-01-01', '2007-01-01'), None)

    def test_validate_date_format_failure(self):
        assert validate_date_format('2007-01-01', ('2007-2007-01', '2007-2007-01')) == \
            (False, "2007-01-01 to ('2007-2007-01', '2007-2007-01') is not a date")

    def test_month_name_to_number_string(self):
        assert month_name_to_number_string('jan') == '01'
        assert month_name_to_number_string('feb') == '02'
        assert month_name_to_number_string('mar') == '03'
        assert month_name_to_number_string('nar') == '03'  # for MGI typo MGI:2155139
        assert month_name_to_number_string('apr') == '04'
        assert month_name_to_number_string('may') == '05'
        assert month_name_to_number_string('jun') == '06'
        assert month_name_to_number_string('jul') == '07'
        assert month_name_to_number_string('aug') == '08'
        assert month_name_to_number_string('sep') == '09'
        assert month_name_to_number_string('oct') == '10'
        assert month_name_to_number_string('nov') == '11'
        assert month_name_to_number_string('dec') == '12'
