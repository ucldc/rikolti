import requests
import json
from collections import Counter
from typing import Optional
from itertools import chain
from deepdiff import DeepDiff
import difflib
from dataclasses import dataclass, asdict


from rikolti.record_indexer import settings
from rikolti.record_indexer.utils import print_opensearch_error
from rikolti.utils.versions import get_versioned_page_as_json
from rikolti.utils.versions import create_summary_report_version
from rikolti.utils.versions import create_detail_report_version
from rikolti.utils.versions import put_markdown_report
from rikolti.utils.versions import get_version

# compare IDs

def get_indexed_ids_from_opensearch(collection_id: int) -> dict[str, str]:
    """
    Get the complete list of calisphere-ids currently indexed for a given collection id.
    """
    opensearch_request = {
        "query": {
            "term": {
                "collection_url": f"{collection_id}"
            }
        },
        "_source": ["calisphere-id"],
        "size": 1000,
        "track_total_hits": True
    }

    search_url = f"{settings.ENDPOINT}/rikolti-prd/_search"
    resp = requests.get(
        url=search_url,
        params={"scroll": "1m"},
        data=json.dumps(opensearch_request),
        headers={"Content-Type": "application/json"},
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= resp.status_code <= 299):
        print_opensearch_error(resp, search_url)
        resp.raise_for_status()

    response_data = resp.json()

    if response_data['hits']['total']['value'] == 0:
        print(
            f"No records found in OpenSearch for collection {collection_id}"
        )
        return {}
    

    scroll_id = resp.headers.get('X-Scroll-Id')
    while scroll_id:
        scroll_resp = requests.post(
            url=f"{settings.ENDPOINT}/_search/scroll",
            data=json.dumps({
                "scroll_id": scroll_id
            }),
            headers={"Content-Type": "application/json"},
            auth=settings.get_auth(),
            verify=settings.verify_certs()
        )
        if not (200 <= scroll_resp.status_code <= 299):
            print_opensearch_error(scroll_resp, search_url)
            scroll_resp.raise_for_status()
        print(scroll_resp)
        scroll_data = scroll_resp.json()
        if not scroll_data['hits']['hits']:
            break
        response_data['hits']['hits'].extend(scroll_data['hits']['hits'])
        scroll_id = scroll_resp.headers.get('X-Scroll-Id')

    indexed_ids = {
        r['_id']: r['_source']['calisphere-id']
        for r in response_data['hits']['hits'] 
    }
    
    if len(indexed_ids) != response_data['hits']['total']['value']:
        print(
            f"Warning: Retrieved {len(indexed_ids)} indexed IDs, "
            f"but total hits reported as "
            f"{response_data['hits']['total']['value']}"
        )
    return indexed_ids


def _diff_ids(indexed_records, candidate_records):
    """
    Returns sets of IDs that are only in indexed_records and only in candidate_records.
    """
    # compare the ids used at calisphere.org/item/<id>
    # indexed_records.values() is the calisphere-id, used in harvesting
    indexed_ids = indexed_records.keys()
    candidate_ids = candidate_records.keys()

    indexed_set = set(indexed_ids)
    candidate_set = set(candidate_ids)

    only_in_indexed = indexed_set - candidate_set
    only_in_candidate = candidate_set - indexed_set

    return only_in_indexed, only_in_candidate


def create_id_diff_report(
        indexed_records: dict[str, str], 
        candidate_records: dict[str, str], 
        last_mapped_version: Optional[str]
    ) -> list[str]:
    """
    Report out IDs that are indexed, but not in candidate, and vice versa. 
    Returns a string summary of the differences.
    """
    only_in_indexed, only_in_candidate = _diff_ids(indexed_records, candidate_records)

    report = ["# ID Differences\n",]
    if only_in_indexed or only_in_candidate:
        if last_mapped_version:
            report = [
                f"Since last mapped {last_mapped_version} version:",
                f"    {len(only_in_indexed)} records were dropped, or changed ID",
                f"    {len(only_in_candidate)} records were picked up, or changed ID",
            ]
        else:
            report = [
                "No previously mapped version found:",
                f"    {len(only_in_indexed)} records were dropped, or changed ID",
                f"    {len(only_in_candidate)} records were picked up, or changed ID",
            ]
    else: 
        report.extend([
            "No ID differences found between indexed and candidate metadata.",
            f"All {len(indexed_records.keys())} records match."
        ])

    if only_in_indexed:
        report.extend([
            "\n## IDs found only in the indexed metadata:",
            f"Found {len(indexed_records.keys())} total records in the index.",
            f"{len(indexed_records.keys()) - len(only_in_indexed)} records matched candidate metadata."
            f"Missing {len(only_in_indexed)} records from candidate metadata (dropped or changed)"
        ])
        for idx_id in sorted(only_in_indexed):
            report.append(f"        - {indexed_records[idx_id]} | {idx_id}")
    if only_in_candidate:
        report.extend([
            "\n## IDs found only in the candidate metadata:",
            f"Found {len(candidate_records.keys())} total records in the candidate metadata.",
            f"{len(candidate_records.keys()) - len(only_in_candidate)} records matched indexed metadata."
            f"Identified {len(only_in_candidate)} new records in candidate metadata (new or changed)"
        ])
        for cand_id in sorted(only_in_candidate):
            report.append(f"        - {candidate_records[cand_id]} | {cand_id}")

    return report

# get basis of comparison for DIFFs

def _get_indexed_data_from_opensearch(calisphere_ids: list[str]):
    search_url = f"{settings.ENDPOINT}/rikolti-prd/_search"
    query = {
        "query": {
            "terms": {
                "calisphere-id": calisphere_ids
            }
        },
        "size": len(calisphere_ids) * 2
    }
    resp = requests.post(
        url=search_url,
        data=json.dumps(query),
        headers={"Content-Type": "application/json"},
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= resp.status_code <= 299):
        print_opensearch_error(resp, search_url)
        resp.raise_for_status()
    response_data = resp.json()
    indexed_records = [r['_source'] for r in response_data['hits']['hits']]
    indexed_records = {r['calisphere-id']: r for r in indexed_records}

    return indexed_records

def strip_non_mapping_fields(indexed_metadata: dict, candidate_metadata: dict):
    """
    removes fields from indexed metadata that are not part of the mapping
    so they don't interfere with diffing
    """
    # not exactly apples-to-apples here, so fudge the mapped and indexed
    # records for backwards compatibility
    for _, record in candidate_metadata.items():
        candidate_removals = ['is_shown_at', 'is_shown_by', 'item_count', 'media_source', 'thumbnail_source']
        for field in candidate_removals:
            if field in record.keys():
                record.pop(field)

    for _, record in indexed_metadata.items():
        indexed_removals = ['rikolti', 'thumbnail', 'media']
        for field in indexed_removals:
            if field in record.keys():
                record.pop(field)

    return indexed_metadata, candidate_metadata

def _get_mapped_pages_from_opensearch(calisphere_ids: list[str]):
    search_url = f"{settings.ENDPOINT}/rikolti-prd/_search"
    query = {
        "query": {
            "terms": {
                "calisphere-id": calisphere_ids
            }
        },
        "_source": ["calisphere-id", "rikolti"],
        "size": len(calisphere_ids) * 2
    }
    resp = requests.post(
        url=search_url,
        data=json.dumps(query),
        headers={"Content-Type": "application/json"},
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= resp.status_code <= 299):
        print_opensearch_error(resp, search_url)
        resp.raise_for_status()
    response_data = resp.json()
    indexed_records = [r['_source'] for r in response_data['hits']['hits']]
    indexed_records = {
        r['calisphere-id']: r['rikolti'] for r in indexed_records
    }
    mapped_pages = [rikolti['page'] for rikolti in indexed_records.values()]
    c = Counter(mapped_pages)
    print(f"getting mapped pages from open search: {c}")
    return c

def get_basis_of_comparison(indexed_version: Optional[str], candidate_record_ids: list[str]) -> dict[str, dict]:
    if not indexed_version:
        indexed_records = {}
    elif indexed_version == 'initial':
        indexed_records = _get_indexed_data_from_opensearch(
            list(candidate_record_ids))
    else:
        indexed_mapped_pages = (
            _get_mapped_pages_from_opensearch(list(candidate_record_ids))
        )
        indexed_mapped_version = indexed_version.split('/with_content_urls')[0]

        indexed_records = {}
        for indexed_mapped_page in indexed_mapped_pages:
            print(f"getting versioned page at {indexed_mapped_version}/data/{indexed_mapped_page}")
            version_page = get_versioned_page_as_json(
                f"{indexed_mapped_version}/data/{indexed_mapped_page}"
            )
            for r in version_page:
                if r['calisphere-id'] in candidate_record_ids:
                    indexed_records[r['calisphere-id']] = r
    
    return indexed_records

# missing fields reporting

def check_for_missing_fields(candidate_record: dict) -> dict[str, list[str]]:
    """
    Check candidate record for missing required mapping fields.
    Returns a list of missing fields, or an empty list if none are missing.
    """
    required_fields = [
        'id',
        'calisphere-id',
        'title',
        'type',
        'rights',
        'is_shown_by',
        'is_shown_at',
    ]
    missing_fields = []
    for field in required_fields:
        if field not in candidate_record or candidate_record[field] in [None, '']:
            missing_fields.append(field)

    return missing_fields

def create_missing_fields_report(missing: dict[str, list[str]], total_number: int) -> tuple[list[str], list[str]]:
    """
    Create a report of missing required fields in candidate records.
    Returns a list of strings summarizing the missing fields.
    """
    summary_report = ["\n# Required Fields",]

    if any(missing.values()):
        summary_report.append("\n## Records with missing Required Fields:")
    else:
        summary_report.extend([
            f"All {total_number} candidate records contain all required fields.",
            "Analyzed the following required fields: "
            f"{', '.join(missing.keys())}"
        ])
        return summary_report, summary_report

    detail_report = summary_report.copy()
    summary_limit = 3

    for field, ids in missing.items():
        heading = f"\n### {field}"
        summary_report.append(heading)
        detail_report.append(heading)

        if ids:
            summary_report.append(f"[{len(ids)}/{total_number}] Field '{field}' is missing in {len(ids)}/{total_number} records:")
            detail_report.append(f"[{len(ids)}/{total_number}] Field '{field}' is missing in {len(ids)}/{total_number} records:")
            summary_report.extend([f"    - {record_id[0]} | {record_id[1]}" for i, record_id in enumerate(ids) if i < summary_limit])
            if len(ids) > summary_limit:
                summary_report.append(f"    - ...and {len(ids) - summary_limit} more.")
            detail_report.extend([f"    - {record_id[0]} | {record_id[1]}" for record_id in ids])
        else:
            summary_report.append(f"All records contain the required field '{field}'.")
            detail_report.append(f"All records contain the required field '{field}'.")

    return summary_report, detail_report

# create report
def get_indexed_version(collection_id: int) -> Optional[str]:
    """
    queries OpenSearch for the currently indexed mapped metadata version
    for a given collection id.
    returns the version path as a str, or None if no data is indexed.
    raises an exception if multiple versions are found.
    raises an exception if a malformed version is found.
    """
    opensearch_request = {
        "query": {
            "term": {
                "collection_url": f"{collection_id}"
            }
        },
        "size": 1,
        "track_total_hits": True, 
        "aggs": {
            "metadata_version": {
                "terms": {
                    "field": "rikolti.version_path",
                    "size": 100
                }
            }
        }
    }

    search_url = f"{settings.ENDPOINT}/rikolti-prd/_search"
    resp = requests.post(
        url=search_url,
        data=json.dumps(opensearch_request),
        headers={"Content-Type": "application/json"},
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= resp.status_code <= 299):
        print_opensearch_error(resp, search_url)
        resp.raise_for_status()

    response_data = resp.json()

    indexed_version = response_data.get('aggregations', {}) \
        .get('metadata_version', {}) \
        .get('buckets', [])
    if len(indexed_version) == 0 and response_data['hits']['total']['value'] == 0:
        print(
            "OpenSearch Request for Basis of Comparison: \n" + 
            json.dumps(opensearch_request, indent=2) + "\n\n" +
            "Response: \n" +
            json.dumps(response_data, indent=2) + "\n\n" +
            "*" * 80 + "\n" +
            f"No indexed data found for collection {collection_id}, skipping diff.\n" +
            "*" * 80 + "\n"
        )
        return None
    if len(indexed_version) > 1:
        raise ValueError(
            f"Multiple currently indexed mapped metadata versions found for "
            f"collection {collection_id}, please investigate: \n" + 
            json.dumps(response_data, indent=2) + "\nRequest: \n" + 
            json.dumps(opensearch_request, indent=2)
        )
    indexed_version = indexed_version[0]['key']
    if indexed_version != 'initial' and '/with_content_urls' not in indexed_version:
        raise ValueError(
            f"Currently indexed mapped metadata version for collection "
            f"{collection_id} is invalid: \n" + json.dumps(response_data, indent=2) + 
            "\nRequest: \n" + json.dumps(opensearch_request, indent=2)
        )
    return indexed_version


def print_other_diff_warning(diffs):
    # diffs is a dict of calisphere-id -> DeepDiff result with the values_changed key already popped
    other_kinds_of_diffs = [deep_diff.keys() for deep_diff in diffs.values() if deep_diff.keys()]
    flattened_list = list(chain.from_iterable(other_kinds_of_diffs))
    deduped_list = list(set(flattened_list))
    
    if deduped_list:
        print("*" * 80)
        print("Other kinds of diffs found besides 'values_changed' - investigate:")
        print('\n    '.join(deduped_list))
        print("*" * 80)


def string_diff(old_value, new_value):
    """
    Create a git-style diff showing what was removed and added between two strings.
    Returns a string like '-Z ++00:00' for changes, or the full change if complex.
    Falls back to 'old -> new' format when strings are too different.
    """
    if not isinstance(old_value, str) or not isinstance(new_value, str):
        return f"{old_value} -> {new_value}"
    
    # Use SequenceMatcher to find matching blocks
    matcher = difflib.SequenceMatcher(None, old_value, new_value)
    
    # If similarity is too low, just show full values
    if matcher.quick_ratio() < 0.7:
        return f"{old_value} -> {new_value}"
    
    opcodes = matcher.get_opcodes()
    
    diff_parts = []
    for tag, i1, i2, j1, j2 in opcodes:
        if tag == 'replace':
            diff_parts.append(f"-{old_value[i1:i2]} +{new_value[j1:j2]}")
        elif tag == 'delete':
            diff_parts.append(f"-{old_value[i1:i2]}")
        elif tag == 'insert':
            diff_parts.append(f"+{new_value[j1:j2]}")
        # 'equal' blocks are skipped
    
    if diff_parts:
        diff_str = ' '.join(diff_parts)
        # If the diff is longer or nearly as long as showing both values, use simple format
        simple_str = f"{old_value} -> {new_value}"
        if len(diff_str) >= len(simple_str) * 0.8:
            return simple_str
        return diff_str
    else:
        return "no change"


@dataclass(frozen=True)
class DiffReportStatus:
    summary_report_filepath: Optional[str] = None
    detail_report_filepath: Optional[str] = None
    mapped_version: Optional[str] = None
    error: bool = False
    exception: Optional[Exception] = None

    def asdict(self):
        d = asdict(self)
        if self.exception:
            d['exception'] = str(self.exception)
        return d


def create_reports(collection_id, mapped_pages: list[str]) -> tuple[list[str], list[str]]:
    # returns summary_report_content, detail_report_content
    indexed_version = get_indexed_version(collection_id)
    diff_stats = {}
    required_fields = [
        'id',
        'calisphere-id',
        'title',
        'type',
        'rights',
        'is_shown_by',
        'is_shown_at',
    ]
    missing_fields_stats = {field: [] for field in required_fields}
    candidate_ids = {}
    indexed_ids = get_indexed_ids_from_opensearch(collection_id)

    # do a page-by-page comparison for missing required fields and
    # field-level diffs to avoid memory issues, but go ahead and make
    # a complete list of candidate ids while we're at it.
    for candidate_page in mapped_pages:
        candidate_records = get_versioned_page_as_json(candidate_page)
        candidate_ids.update({r['id']: r['calisphere-id'] for r in candidate_records})
        candidate_records = {r['calisphere-id']: r for r in candidate_records}

        indexed_records = get_basis_of_comparison(indexed_version, list(candidate_records.keys()))
        if indexed_version == 'initial':
            indexed_records, candidate_records = strip_non_mapping_fields(
                indexed_records, candidate_records)

        for candidate_record_id in candidate_records:
            missing_fields = check_for_missing_fields(candidate_records[candidate_record_id])
            if missing_fields:
                for field in missing_fields:
                    missing_fields_stats[field].append((candidate_record_id, candidate_records[candidate_record_id]['id']))

            if candidate_record_id in indexed_records:
                record_diff = DeepDiff(
                    indexed_records[candidate_record_id],
                    candidate_records[candidate_record_id],
                    ignore_order=True
                )
                if record_diff:
                    diff_stats[candidate_record_id] = {
                        "id": candidate_records[candidate_record_id]['id'],
                        "deep_diff": record_diff
                    }
    
    indexed_ids_report = create_id_diff_report(indexed_ids, candidate_ids, indexed_version)

    summary_missing_fields_report, detail_missing_fields_report = create_missing_fields_report(missing_fields_stats, len(candidate_ids))

    if not diff_stats:
        print(f"No diffs found between mapped version and indexed version for collection {collection_id}.")

    # let's just look at values changed for now, I haven't seen other kinds of diffs
    # values_changed = {cali_id: {"id": diff["id"], "values_changed": diff["deep_diff"].pop('values_changed', {})}
    #                         for cali_id, diff in diff_stats.items()}
    # print_other_diff_warning(diff_stats)

    # make git-style string diffs for easier aggregation
    values_changed = {
        cali_id: {
            "id": d['id'],
            "deep_diff": d['deep_diff'],
            "str_diffs": [
                f"{field}: {string_diff(values['old_value'], values['new_value'])}"
                for field, values in d['deep_diff']['values_changed'].items()
            ]
        }
        for cali_id, d, in diff_stats.items()
    }

    diff_summary_report, diff_details_report = create_aggregate_diff_reports(values_changed)

    summary_report = indexed_ids_report + ["\n" + "-"*80] + summary_missing_fields_report + ["\n" + "-"*80] + diff_summary_report
    detail_report = indexed_ids_report + ["\n" + "-"*80] + detail_missing_fields_report + ["\n" + "-"*80] + diff_details_report

    return summary_report, detail_report


def create_qa_reports(collection_id, mapped_pages: list[str]):
    summary_report_content, detail_report_content = create_reports(collection_id, mapped_pages)

    summary_report_content = '\n'.join(summary_report_content)
    detail_report_content = '\n'.join(detail_report_content)

    mapped_version = get_version(collection_id, mapped_pages[0])

    summary_report_location = create_summary_report_version(mapped_version)
    put_markdown_report(summary_report_content, summary_report_location)

    detail_report_location = create_detail_report_version(mapped_version)
    put_markdown_report(detail_report_content, detail_report_location)

    return summary_report_location, detail_report_location

# diff report

def create_aggregate_diff_reports(values_changed):
    summary_limit = 3

    # Count how many records have each diff
    diff_counter = Counter()
    for d in values_changed.values():
        for str_diff in d['str_diffs']:
            diff_counter[str_diff] += 1
    
    total_records = len(values_changed.keys())
    
    # Categorize diffs by frequency
    diffs_in_every_record = {diff for diff, count in diff_counter.items() if count == total_records}
    diffs_in_most_records = {diff for diff, count in diff_counter.items() if count >= total_records * 0.8 and count < total_records}
    diffs_in_multiple_records = {diff for diff, count in diff_counter.items() if count > 1 and count < total_records * 0.8}

    diff_summary_report = ["\n# Diffs"]
    diff_details_report = ["\n# Diffs"]

    # Report diffs in all records
    heading = f"\n## Diffs in ALL {total_records} records:"
    diff_summary_report.append(heading)
    diff_details_report.append(heading)
    for diff in sorted(diffs_in_every_record):
        diff_summary_report.append(f"  {diff}")
        diff_details_report.append(f"  {diff}")
        # remove diff from individual record reports
        for cali_id in values_changed:
            values_changed[cali_id]['str_diffs'].remove(diff)
            field = diff.split(":")[0]
            values_changed[cali_id]['deep_diff']['values_changed'].pop(field)

    # Print diffs in most records
    if diffs_in_most_records:
        heading = f"\n## Diffs in MOST records (≥80%, at least {int(total_records*.8)} of {total_records} records):"
        diff_summary_report.append(heading)
        diff_details_report.append(heading)
        for diff in sorted(diffs_in_most_records):
            count = diff_counter[diff]
            pct = (count / total_records) * 100
            field = diff.split(":")[0]

            diff_summary_report.append(f"  [{count}/{total_records} = {pct:.1f}%] {diff}")
            diff_details_report.append(f"\n### {field}")
            diff_details_report.append(f"[{count}/{total_records} = {pct:.1f}%] {diff}")

            print_example = True
            summary_count = 0
            print_ellipses = True

            for cali_id in values_changed:
                if diff in values_changed[cali_id]['str_diffs']:
                    if print_example:
                        diff_summary_report.append(f"    ex: {values_changed[cali_id]['deep_diff']['values_changed'][field]}")
                        diff_details_report.append(f"ex: {values_changed[cali_id]['deep_diff']['values_changed'][field]}")
                        diff_summary_report.append(f"    except in {total_records-count} records:")
                        diff_details_report.append(f"except in {total_records-count} records:")
                        print_example = False
                    values_changed[cali_id]['str_diffs'].remove(diff)
                    values_changed[cali_id]['deep_diff']['values_changed'].pop(field)
                else:
                    diff_details_report.append(f"    - {cali_id} | {values_changed[cali_id]['id']}")
                    if summary_count < summary_limit:
                        diff_summary_report.append(f"      - {cali_id} | {values_changed[cali_id]['id']}")
                        summary_count += 1
                    elif summary_count == summary_limit and print_ellipses:
                        diff_summary_report.append("        ...")
                        print_ellipses = False

    # Print diffs in multiple records    
    if diffs_in_multiple_records:
        heading = f"\n## Diffs in MULTIPLE records (<80%, 2-{int(total_records*0.8)} records of {total_records}):"
        diff_summary_report.append(heading)
        diff_details_report.append(heading)

        # Sort by count descending
        for diff, count in sorted([(d, diff_counter[d]) for d in diffs_in_multiple_records], 
                                   key=lambda x: x[1], reverse=True):
            pct = (count / total_records) * 100
            field = diff.split(":")[0]

            diff_summary_report.append(f"  [{count}/{total_records} = {pct:.1f}%] {diff}")
            diff_details_report.append(f"\n### {field}")
            diff_details_report.append(f"[{count}/{total_records} = {pct:.1f}%] {diff}")

            print_example = True
            summary_count = 0
            print_ellipses = True

            for cali_id in values_changed:
                if diff in values_changed[cali_id]['str_diffs']:
                    if print_example:
                        diff_summary_report.append(f"    ex: {values_changed[cali_id]['deep_diff']['values_changed'][field]}")
                        diff_details_report.append(f"ex: {values_changed[cali_id]['deep_diff']['values_changed'][field]}")
                        diff_summary_report.append(f"    in {count} records:")
                        diff_details_report.append(f"in {count} records:")
                        print_example = False

                    diff_details_report.append(f"    - {cali_id} | {values_changed[cali_id]['id']}")
                    if summary_count < summary_limit:
                        diff_summary_report.append(f"      - {cali_id} | {values_changed[cali_id]['id']}")
                        summary_count += 1
                    if summary_count == summary_limit and print_ellipses:
                        diff_summary_report.append("        ...")
                        print_ellipses = False

                    values_changed[cali_id]['str_diffs'].remove(diff)
                    values_changed[cali_id]['deep_diff']['values_changed'].pop(field)

    # cleanup empty deep diffs:
    for cali_id in values_changed:
        values_changed[cali_id]['deep_diff'] = {
            k: v for k, v in values_changed[cali_id]['deep_diff'].items() if v
        }
    # cleanup empty diffs:
    record_specific_diffs = {
        cali_id: diffs for cali_id, diffs in values_changed.items() if diffs['str_diffs'] or diffs['deep_diff']
    }
    heading = f"\n## Record-specific diffs (unique or uncommon) {len(record_specific_diffs.keys())} of {total_records} records:"
    diff_summary_report.append(heading)
    diff_details_report.append(heading)

    if len(record_specific_diffs) == 0:
        diff_summary_report.append("    None")
        diff_details_report.append("    None")
    else:
        def set_encoder(obj):
            if isinstance(obj, set):
                return list(obj)
            if hasattr(obj, '__iter__') and type(obj).__name__ in ('SetOrdered'):
                # DeepDiff uses 'SetOrdered'
                return list(obj)
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

        diff_summary_report.append(json.dumps(record_specific_diffs, indent=2, default=set_encoder))
        diff_details_report.append(json.dumps(record_specific_diffs, indent=2, default=set_encoder))
    
    return diff_summary_report, diff_details_report


