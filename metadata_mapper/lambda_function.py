import importlib
import json
import logging
import sys
from typing import Union
from urllib.parse import parse_qs, urlparse

from . import settings
from .mappers.mapper import Record, UCLDCWriter, Vernacular
from rikolti.utils.rikolti_storage import RikoltiStorage

logger = logging.getLogger(__name__)


def import_vernacular_reader(mapper_type):
    """
    accept underscored_module_name_prefixes
    accept CamelCase class name prefixes split on underscores
    for example:
    mapper_type       | mapper module       | class name
    ------------------|---------------------|------------------
    nuxeo.nuxeo       | nuxeo_mapper        | NuxeoVernacular
    oai.content_dm    | content_dm_mapper   | ContentDmVernacular
    """
    *mapper_parent_modules, snake_cased_mapper_name = mapper_type.split(".")

    mapper_module = importlib.import_module(
        f".mappers.{'.'.join(mapper_parent_modules)}.{snake_cased_mapper_name}_mapper",
        package=__package__
    )

    mapper_type_words = snake_cased_mapper_name.split('_')
    class_type = ''.join([word.capitalize() for word in mapper_type_words])
    vernacular_class = getattr(
        mapper_module, f"{class_type}Vernacular")

    if not issubclass(vernacular_class, Vernacular):
        print(f"{mapper_type} not a subclass of Vernacular", file=sys.stderr)
        exit()
    return vernacular_class


def parse_enrichment_url(enrichment_url):
    enrichment_func = (urlparse(enrichment_url)
                       .path
                       .strip('/')
                       .replace('-', '_'))
    kwargs = parse_qs(urlparse(enrichment_url).query)
    if enrichment_func not in dir(Record):
        if settings.SKIP_UNDEFINED_ENRICHMENTS:
            return None, None
        else:
            raise Exception(f"ERROR: {enrichment_func} not found in {Record}")
    return enrichment_func, kwargs


def run_enrichments(records, collection, enrichment_set, page_filename):
    for enrichment_url in collection.get(enrichment_set, []):
        enrichment_func, kwargs = parse_enrichment_url(enrichment_url)
        if not enrichment_func and settings.SKIP_UNDEFINED_ENRICHMENTS:
            continue
        if enrichment_func in ['required_values_from_collection_registry',
                               'set_ucldc_dataprovider']:
            kwargs.update({'collection': collection})
        logging.debug(
            f"[{collection['id']}]: running enrichment: {enrichment_func} "
            f"for page {page_filename} with kwargs: {kwargs}")
        records = [
            record.enrich(enrichment_func, **kwargs)
            for record in records
        ]
    return records


def map_page(collection_id: int, page_filename: str, collection: Union[dict, str]):
    if isinstance(collection, str):
         collection = json.loads(collection)

    vernacular_reader = import_vernacular_reader(
        collection.get('rikolti_mapper_type'))
    storage = RikoltiStorage(f"{settings.DATA_SRC_URL}/{collection_id}/vernacular_metadata/{page_filename}")
    api_resp = storage.get_page_content()

    source_vernacular = vernacular_reader(collection_id, page_filename)
    source_metadata_records = source_vernacular.parse(api_resp)

    source_metadata_records = run_enrichments(
        source_metadata_records, collection, 'rikolti__pre_mapping', page_filename)

    for record in source_metadata_records:
        record.to_UCLDC()
    mapped_records = source_metadata_records

    writer = UCLDCWriter(collection_id, page_filename)
    # TODO: write interim mapped but not enriched metadata to s3?
    # if settings.DATA_DEST["STORE"] == 'file':
    #     writer.write_local_mapped_metadata(
    #         [record.to_dict() for record in mapped_records])

    mapped_records = run_enrichments(
        mapped_records, collection, 'rikolti__enrichments', page_filename)

    # TODO: analyze and simplify this straight port of the
    # solr updater module into the Rikolti framework
    mapped_records = [record.solr_updater() for record in mapped_records]
    mapped_records = [record.remove_none_values() for record in mapped_records]

    group_page_exceptions = {}
    page_exceptions = {
        rec.legacy_couch_db_id: rec.enrichment_report
        for rec in mapped_records if rec.enrichment_report
    }
    if page_exceptions:
        # Group like lists of enrichment chain errors
        for couch_id, reports in page_exceptions.items():
            report = " | ".join(reports)
            group_page_exceptions.setdefault(report, []).append(couch_id)

    # some enrichments had previously happened at ingest into Solr
    # TODO: these are just two, investigate further
    # mapped_records = [record.add_sort_title() for record in mapped_records]
    # mapped_records = [record.map_registry_data(collection)
    #                   for record in mapped_records]

    mapped_metadata = [record.to_dict() for record in mapped_records]
    if settings.DATA_DEST["STORE"] == 'file':
        writer.write_local_mapped_metadata(mapped_metadata)
    else:
        writer.write_s3_mapped_metadata(mapped_metadata)

    return {
        'status': 'success',
        'num_records_mapped': len(mapped_records),
        'page_exceptions': group_page_exceptions,
        'page_filename': page_filename,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('collection_id', help='collection id')
    parser.add_argument('page_filename', help='vernauclar metadata page filename')
    parser.add_argument('collection', help='json collection metadata from registry')

    args = parser.parse_args(sys.argv[1:])
    mapped_page = map_page(args.collection_id, args.page_filename, args.collection)

    print(f"{mapped_page.get('num_records_mapped')} records mapped")

    for report, couch_ids in mapped_page.get('exceptions', {}).items():
        print(f"{len(couch_ids)} records report enrichments errors: {report}")
        print(f"check the following ids for issues: {couch_ids}")

