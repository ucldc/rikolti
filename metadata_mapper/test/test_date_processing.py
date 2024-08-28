import csv
import sys
import os
import ast

# get absolute path to parent of rikolti folder, four folders up
rikolti_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if rikolti_path not in sys.path:
    sys.path.append(rikolti_path)

from rikolti.metadata_mapper.mappers.solr_updater_helpers import (
    get_facet_decades, unpack_display_date, make_sort_dates)
from rikolti.metadata_mapper.mappers.date_enrichments import convert_dates


def enumerate_cases_from_csv(csv_filename):
    test_data_path = os.path.join(
        rikolti_path, "rikolti", "metadata_mapper", "test", "date_data")
    with open(os.path.join(test_data_path, csv_filename)) as csvfile:
        reader = csv.reader(csvfile, delimiter="|")
        header = next(reader)
        header = [cell.strip() for cell in header]
        for row_number, row in enumerate(reader):
            for column, cell in enumerate(row):
                cell = cell.strip()
                if cell.startswith(f"{header[column]}="):
                    cell = cell[len(f"{header[column]}="):]
                    cell = ast.literal_eval(cell)
                row[column] = cell
            yield row_number, row


def test_add_facet_decade():
    filename = "add_facet_decade.csv"
    for index, row in enumerate_cases_from_csv(filename):
        collection_id, _, date_value, facet_decades, __ = row
        assertion_error = (
            f"Test case {index} failed - Collection {collection_id}\n"
            f"{'Input:':<20}{date_value}\n"
            f"{'Expected Output:':<20}{facet_decades}"
        )
        assert (
            set(get_facet_decades(date_value)) == set(facet_decades)
        ), assertion_error


def test_enrich_date():
    filename = "enrich_date.csv"
    for index, row in enumerate_cases_from_csv(filename):
        collection_id, _, date_values, converted_dates = row
        assertion_error = (
            f"Test case {index} failed - Collection {collection_id}\n"
            f"{'Input:':<20}{date_values}\n"
            f"{'Expected Output:':<20}{converted_dates}"
        )
        assert convert_dates(date_values) == converted_dates, assertion_error


def test_enrich_earliest_date():
    filename = "enrich_earliest_date.csv"
    for index, row in enumerate_cases_from_csv(filename):
        collection_id, _, date_values, converted_dates = row
        assertion_error = (
            f"Test case {index} failed - Collection {collection_id}\n"
            f"{'Input:':<20}{date_values}\n"
            f"{'Expected Output:':<20}{converted_dates}"
        )
        actual_output = convert_dates(date_values)

        # for some reason, convert_dates does not seem to return
        # values in a reliable order, sort both actual output and expected
        # output before comparison
        if isinstance(actual_output, list) and isinstance(converted_dates, list):
            converted_dates = (
                converted_dates.sort(key=lambda d: d.get('displayDate')))
            actual_output = (
                actual_output.sort(key=lambda d: d.get('displayDate')))
            assert actual_output == converted_dates, assertion_error
        else:
            assert actual_output == converted_dates, assertion_error

def test_sort_dates():
    filename = "sort_dates.csv"
    for index, row in enumerate_cases_from_csv(filename):
        collection_id, _, date_source, start_date, end_date = row
        expected_output = (start_date, end_date)
        assertion_error = (
            f"Test case {index} failed - Collection {collection_id}\n"
            f"{'Input:':<20}{date_source}\n"
            f"{'Expected Output:':<20}{expected_output}"
        )
        assert make_sort_dates(date_source) == expected_output, assertion_error


def test_unpack_display_date():
    filename = "unpack_display_date.csv"
    for index, row in enumerate_cases_from_csv(filename):
        collection_id, _, date_obj, dates = row
        assertion_error = (
            f"Test case {index} failed - Collection {collection_id}\n"
            f"{'Input:':<20}{date_obj}\n"
            f"{'Expected Output:':<20}{dates}"
        )
        assert unpack_display_date(date_obj) == dates, assertion_error

