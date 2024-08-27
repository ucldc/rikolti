import re
from datetime import date, datetime, timezone

# date_file = open(
#     f"/usr/local/airflow/rikolti_data/date_cases/{collection_id}_dates.txt",
#     "a"
# )

def unpack_display_date(date_obj):
    '''Unpack a couchdb date object'''
    if not isinstance(date_obj, list):
        date_obj = [date_obj]

    dates = []
    for dt in date_obj:
        if isinstance(dt, dict):
            displayDate = dt.get('displayDate', None)
        elif isinstance(dt, str):
            displayDate = dt
        else:
            displayDate = None
        dates.append(displayDate)
    # print(f"unpack_display_date | {date_obj=} | {dates=}", file=date_file)
    return dates


def get_facet_decades(date_value):
    '''Return set of decade string for given date structure.
    date is a dict with a "displayDate" key.
    '''
    if isinstance(date_value, dict):
        facet_decades = facet_decade(
            date_value.get('displayDate', ''))
    else:
        facet_decades = facet_decade(str(date_value))
    facet_decade_set = set()  # don't repeat values
    for decade in facet_decades:
        facet_decade_set.add(decade)
    return list(facet_decade_set)


def facet_decade(date_string):
    """ process string and return array of decades """
    year = date.today().year
    pattern = re.compile(r'(?<!\d)(\d{4})(?!\d)')
    matches = [int(match) for match in re.findall(pattern, date_string)]
    matches = list(filter(lambda a: a >= 1000, matches))
    matches = list(filter(lambda a: a <= year, matches))
    if not matches:
        return ['unknown']
    start = (min(matches) // 10) * 10
    end = max(matches) + 1
    return map('{0}s'.format, range(start, end, 10))


def make_sort_dates(date_source):
    dates_start = [make_datetime(dt.get("begin"))
                for dt in date_source
                if isinstance(dt, dict) and dt.get("begin")]
    dates_start = sorted(filter(None, dates_start))

    start_date = \
        dates_start[0].isoformat() if dates_start else None

    dates_end = [make_datetime(dt.get("end"))
                for dt in date_source
                if isinstance(dt, dict) and dt.get("end")]
    dates_end = sorted(filter(None, dates_end))

    # TODO: should this actually be the last date?, dates_end[-1]
    end_date = dates_end[0].isoformat() if dates_end else None

    # fill in start_date == end_date if only one exists
    start_date = end_date if not start_date else start_date
    end_date = start_date if not end_date else end_date

    # print(
    #     f"sort_dates | {date_source=} | {start_date=} | {end_date=}", 
    #     file=date_file
    # )
    return start_date, end_date

def make_datetime(date_string):
    date_time = None

    #  This matches YYYY or YYYY-MM-DD
    match = re.match(
        r"^(?P<year>[0-9]{4})"
        r"(-(?P<month>[0-9]{1,2})"
        r"-(?P<day>[0-9]{1,2}))?$", date_string)
    if match:
        year = int(match.group("year"))
        month = int(match.group("month") or 1)
        day = int(match.group("day") or 1)
        date_time = datetime(year, month, day, tzinfo=timezone.utc)

        try:
            date_time = datetime(year, month, day, tzinfo=timezone.utc)
        except Exception as e:
            print(f"Error making datetime: {e}")
            pass

    return date_time

