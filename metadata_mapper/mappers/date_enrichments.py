"""
This is copied from dpla-ingestion enrich_date.py with some modifications
For example, we know that enrich_earliest_date and enrich_date are never
called with more than one property, so we are able to remove loops over
multiple properties (mapped_data field names). enrich_date and 
enrich_earliest_date produced one dictionary per property of the format:
{displayDate: str, begin: datetime, end: datetime}, so we can also
simplify checks and remove loops over a potential list of dictionaries.

https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/cfe3dcb06008c0c6cb9d8207fe28bfaa1a855e4f/lib/akamod/enrich_date.py#L396
"""

import re
from dateutil.parser import parse as dateutil_parse
from ..dpla_zen import dateparser
import timelib
import datetime

# default date used by dateutil-python to populate absent date elements
# during parse, e.g. "1999" would become "1999-01-01" instead of using the
# current month/day
# Set this to a date far in the future, so we can use it to check if date
# parsing just failed
DEFAULT_DATETIME_STR = "3000-01-01"
DEFAULT_DATETIME = dateutil_parse(DEFAULT_DATETIME_STR)

# normal way to get DEFAULT_DATIMETIME in seconds is:
# time.mktime(DEFAULT_DATETIME.timetuple())
# but it applies the time zone, which should be added to seconds to get
# real GMT(UTC) as simple solution, hardcoded UTC seconds is given
DEFAULT_DATETIME_SECS = 32503680000.0  # UTC seconds for "3000-01-01"


def out_of_range(d):
    ret = None
    try:
        dateutil_parse(d, fuzzy=True, default=DEFAULT_DATETIME)
    except ValueError:
        ret = True

    return ret


# EDTF: http://www.loc.gov/standards/datetime/pre-submission.html
# (like ISO-8601 but doesn't require timezone)
edtf_date_and_time = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def robust_date_parser(d):
    """
    Robust wrapper around some date parsing libs, making a best effort to
    return a single 8601 date from the input string. No range checking is
    performed, and any date other than the first occuring will be ignored.

    We use timelib for its ability to make at least some sense of invalid
    dates, e.g. 2012/02/31 -> 2012/03/03

    We rely only on dateutil.parser for picking out dates from nearly arbitrary
    strings (fuzzy=True), but at the cost of being forgiving of invalid dates
    in those kinds of strings.

    Returns None if it fails
    """
    # Function for a formatted date string, since datetime.datetime.strftime()
    # only works with years >= 1900.
    return_date = lambda d: "%d-%02d-%02d" % (d.year, d.month, d.day)

    # Check for EDTF timestamp first, because it is simple.
    if edtf_date_and_time.match(d):
        try:
            dateinfo = dateutil_parse(d)
            return return_date(dateinfo)
        except TypeError:
            # not parseable by dateutil_parse()
            dateinfo = None
    isodate = dateparser.to_iso8601(d)
    if isodate is None or out_of_range(d):
        try:
            dateinfo = dateutil_parse(d, fuzzy=True, default=DEFAULT_DATETIME)
            if dateinfo.year == DEFAULT_DATETIME.year:
                dateinfo = None
        except Exception:
            try:
                dateinfo = timelib.strtodatetime(d, now=DEFAULT_DATETIME_SECS)
            except ValueError:
                dateinfo = None
            except Exception as e:
                print("Exception %s in %s" % (e, __name__))

        if dateinfo:
            return return_date(dateinfo)

    return isodate


# ie 1970/1971
year_range = re.compile(r"(?P<year1>^\d{4})[-/](?P<year2>\d{4})$")
# ie 1970-08-01/02
day_range = re.compile(
    r"(?P<year>^\d{4})[-/](?P<month>\d{1,2})[-/](?P<day_begin>\d{1,2})[-/](?P<day_end>\d{1,2}$)"
)
# ie 1970-90
circa_range = re.compile(
    r"(?P<century>\d{2})(?P<year_begin>\d{2})[-/](?P<year_end>\d{1,2})")
# ie 9-1970
month_year = re.compile(r"(?P<month>\d{1,2})[-/](?P<year>\d{4})")
# ie 195-
decade_date = re.compile(r"(?P<year>\d{3})-")
# ie 1920s
decade_date_s = re.compile(r"(?P<year>\d{4})s")
# ie between 2000 and 2002
between_date = re.compile(
    r"between\s*(?P<year1>\d{4})\s*and\s*(?P<year2>\d{4})")



def clean_date(date):
    """Return a given date string without certain characters and expressions"""
    date = date.replace("[", "").replace("]", "").strip(". ")

    regex = [
        (r"\s*to\s*|\s[-/]\s", "-"),        # replace "to", " - ", and " / " with "-"
                                            # remove any spaces around "to"
        (r"[\?\(\)~x]|\s*ca\.?\s*", "")     # replace "?", "(", ")", "~", "x" with ""
                                            # replace "ca" and "ca." with ""
                                            # remove any spaces around "ca" or "ca."
    ]
    if (
        "circa" not in date and "century" not in date and 
        "dec" not in date and "Dec" not in date
    ):
        regex.append((r"\s*c\.?\s*", ""))
    for pattern, replacement in regex:
        date = re.sub(pattern, replacement, date)
    date = date.strip()
    
    if date[-6:] == "-00-00":
        date = date[:-6]
    return date


def parse_date_or_range(date_str):
    # TODO: Handle dates with BC, AD, AH
    #      Handle ranges like 1920s - 1930s
    #      Handle ranges like 11th - 12th century
    start_date, end_date = None, None

    if re.search(r"B\.?C\.?|A\.?D\.?|A\.?H\.?", date_str.upper()):
        pass
    is_edtf_timestamp = edtf_date_and_time.match(date_str)
    hyphen_split = date_str.split("-")
    slash_split = date_str.split("/")
    ellipse_split = date_str.split("..")
    is_hyphen_split = (len(hyphen_split) % 2 == 0)
    is_slash_split = (len(slash_split) % 2 == 0)
    is_ellipse_split = (len(ellipse_split) % 2 == 0)
    if year_range.match(date_str):
        match = year_range.match(date_str)
        start_date, end_date = sorted((match.group("year1"), match.group("year2")))
        return start_date, end_date
    elif (is_hyphen_split or is_slash_split or is_ellipse_split) \
            and not is_edtf_timestamp:
        # We passed over EDTF timestamps because they contain hyphens and we
        # can handle them below.  Note that we don't deal with ranges of
        # timestamps.
        #
        # Handle ranges
        if is_hyphen_split:
            delim = "-"
            split_result = hyphen_split
        elif is_slash_split:
            delim = "/"
            split_result = slash_split
        elif is_ellipse_split:
            delim = ".."
            split_result = ellipse_split
        if day_range.match(date_str):
            # ie 1970-08-01/02
            match = day_range.match(date_str)
            start_date = "%s-%s-%s" % (match.group("year"), match.group("month"),
                              match.group("day_begin"))
            end_date = "%s-%s-%s" % (match.group("year"), match.group("month"),
                              match.group("day_end"))
        elif decade_date.match(date_str):
            match = decade_date.match(date_str)
            start_date = match.group("year") + "0"
            end_date = match.group("year") + "9"
        elif any(
             [0 < len(s) < 4 for s in split_result if len(split_result) == 2]):
            # ie 1970-90, 1970/90, 1970-9, 1970/9, 9/1979
            match = circa_range.match(date_str)
            if match:
                year_begin = match.group("century") + match.group("year_begin")
                year_end = match.group("century") + match.group("year_end")
                if int(year_begin) < int(year_end):
                    # ie 1970-90
                    start_date = robust_date_parser(year_begin)
                    end_date = robust_date_parser(year_end)
                else:
                    # ie 1970-9
                    (y, m) = split_result
                    # If the second number is a month, format it to two digits
                    # and use "-" as the delim for consistency in the
                    # dateparser.to_iso8601 result
                    if int(m) in range(1, 13):
                        date_str = "%s-%02d" % (y, int(m))
                    else:
                        # ie 1970-13
                        # Just use the year
                        date_str = y

                    start_date = robust_date_parser(date_str)
                    end_date = robust_date_parser(date_str)
            else:
                match = month_year.match(date_str)
                if match:
                    date_str = "%s-%02d" % (match.group("year"),
                                     int(match.group("month")))
                    start_date = robust_date_parser(date_str)
                    end_date = robust_date_parser(date_str)
        elif "" in split_result:
            # ie 1970- or -1970 (but not 19uu- nor -19uu)
            s = split_result
            if len(s[0]) == 4 and "u" not in s[0]:
                start_date, end_date = s[0], None
            elif len(s[1]) == 4 and "u" not in s[1]:
                start_date, end_date = None, s[1]
            else:
                start_date, end_date = None, None
        else:
            # ie 1970-01-01-1971-01-01, 1970 Fall/August, 1970 April/May, or
            # wordy date like "mid 11th century AH/AD 17th century (Mughal)"
            date_str = date_str.replace(" ", "")
            date_str = date_str.split(delim)
            begin = delim.join(date_str[:len(date_str) // 2])
            end = delim.join(date_str[len(date_str) // 2:])

            # Check if month in begin or end
            m1 = re.sub(r"[-\d/]", "", begin)
            m2 = re.sub(r"[-\d/]", "", end)
            if m1 or m2:
                # ie 2004July/August, 2004Fall/Winter, or wordy date
                begin, end = None, None

                # Extract year
                for v in date_str:
                    y = re.sub(r"(?i)[a-z]", "", v)
                    if len(y) == 4:
                        begin = y + m1.capitalize()
                        end = y + m2.capitalize()
                        if not dateparser.to_iso8601(begin) or not\
                               dateparser.to_iso8601(end):
                            begin, end = y, y
                        break

            if begin:
                start_date, end_date = robust_date_parser(begin), robust_date_parser(end)
    elif decade_date_s.match(date_str):
        match = decade_date_s.match(date_str)
        year_begin = match.group("year")
        year_end = match.group("year")[:3] + "9"
        start_date, end_date = year_begin, year_end
    elif between_date.match(date_str):
        match = between_date.match(date_str)
        year1 = int(match.group("year1"))
        year2 = int(match.group("year2"))
        start_date, end_date = str(min(year1, year2)), str(max(year1, year2))
    else:
        # This picks up a variety of things, in addition to timestamps.
        parsed = robust_date_parser(date_str)
        start_date, end_date = parsed, parsed

    return start_date, end_date


def test_parse_date_or_range():
    DATE_TESTS = {
        "ca. July 1896": ("1896-07", "1896-07"),  # fuzzy dates
        "c. 1896": ("1896", "1896"),  # fuzzy dates
        "c. 1890-95": ("1890", "1895"),  # fuzzy date range
        "1999.11.01": ("1999-11-01", "1999-11-01"),  # period delim
        "2012-02-31": ("2012-03-02", "2012-03-02"),  # invalid date cleanup
        "12-19-2010": ("2010-12-19", "2010-12-19"),  # M-D-Y
        "5/7/2012": ("2012-05-07", "2012-05-07"),  # slash delim MDY
        "1999 - 2004": ("1999", "2004"),  # year range
        "1999-2004": ("1999", "2004"),  # year range without spaces
        " 1999 - 2004 ": ("1999", "2004"),  # range whitespace
    }
    for i in DATE_TESTS:
        i = clean_date(i)
        res = parse_date_or_range(i)
        assert res == DATE_TESTS[
            i], "For input '%s', expected '%s' but got '%s'" % (
                i, DATE_TESTS[i], res)


def is_year_range_list(date_values):
    """Returns True if value is a list of years in increasing
    order, else False
    """
    return isinstance(date_values, list) and \
        all(v.isdigit() for v in date_values) and \
        date_values == sorted(date_values, key=int) and \
        len(date_values) > 1

def convert_dates(date_values):
    if not date_values:
        return

    if isinstance(date_values, dict):  # already filled in, probably by mapper
        return
    if isinstance(date_values, list):  # remove duplicate values in list
        if any(isinstance(v, dict) for v in date_values):
            return date_values
        else:
            date_values = list(set(date_values))
    if isinstance(date_values, str):   # convert to list if string
        date_values = [date_values]

    dates = []
    if is_year_range_list(date_values):
        dates.append({
            "begin": date_values[0],
            "end": date_values[-1],
            "displayDate": f"{date_values[0]}-{date_values[-1]}"
        })
    else:
        # split on ; and flatten list
        date_values = [
            date for date_str in date_values for date in date_str.split(";")]
        for date in date_values:
            start_date, end_date = parse_date_or_range(clean_date(date))
            if end_date != DEFAULT_DATETIME_STR:
                dates.append({
                    "begin": start_date,
                    "end": end_date,
                    "displayDate": date
                })

    dates.sort(
        key=lambda d: d["begin"] if d["begin"] is not None
        else DEFAULT_DATETIME_STR
    )
    return dates


def check_date_format(record_id, date_value_list):
    """Checks that the begin and end dates are in the proper format, 
    and if not, sets them to None"""
    if not date_value_list:
        return

    for date_value_dict in date_value_list:
        for key, value in date_value_dict.items():
            if value and key != "displayDate":
                try:
                    ymd = [int(s) for s in value.split("-")]
                except ValueError as e:
                    print(f"Invalid date.{key}: {value} - {e} for {record_id}")
                    date_value_dict[key] = None

                year = ymd[0]
                month = ymd[1] if len(ymd) > 1 else 1
                day = ymd[2] if len(ymd) > 2 else 1
                try:
                    datetime.datetime(year=year, month=month, day=day)
                except ValueError as e:
                    print(f"Invalid date.{key}: {value} - {e} for {record_id}")
                    date_value_dict[key] = None
