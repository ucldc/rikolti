import json
import os
from collections import Counter

from .by_record import harvest_record_content

from rikolti.utils.versions import (
    get_mapped_page_content, put_with_content_urls_page, get_version
)


def harvest_page_content(
        collection_id,
        rikolti_mapper_type,
        mapped_page_path,
        with_content_urls_version,
        **kwargs):

    mapped_version = get_version(collection_id, mapped_page_path)
    page_filename = mapped_page_path.split(mapped_version + '/data/')[-1]

    records = get_mapped_page_content(mapped_page_path)
    print(
        f"Harvesting content for {len(records)} records at {mapped_page_path}")

    for i, record in enumerate(records):
        # print(
        #     f"Harvesting record {i}, {record.get('calisphere-id')}, from"
        #     f"{mapped_page_path}"
        # )
        # spit out progress so far if an error has been encountered
        try:
            record_with_content = harvest_record_content(
                record, 
                collection_id, 
                mapped_page_path,
                rikolti_mapper_type
            )
            if not record_with_content.get('thumbnail'):
                warn_level = "ERROR"
                if 'sound' in record.get('type', []):
                    warn_level = "WARNING"
                print(
                    f"{warn_level}: no thumbnail found for {record.get('type')}"
                    f"record {record.get('calisphere-id')} in page {mapped_page_path}"
                )

        except Exception as e:
            print(
                f"Error harvesting record {record.get('calisphere-id')} from"
                f"page {mapped_page_path}; Exiting after harvesting {i} of "
                f"{len(records)} items.\n"
            )
            raise(e)

    put_with_content_urls_page(
        json.dumps(records), page_filename, with_content_urls_version)

    media_source = [r for r in records if r.get('media_source')]
    media_harvested = [r for r in records if r.get('media')]
    media_src_mimetypes = [r.get('media_source', {}).get('mimetype') for r in records]
    media_mimetypes = [r.get('media', {}).get('mimetype') for r in records]

    if media_source:
        print(mapped_page_path)
        print(f"Harvested {len(media_harvested)} media records")
        print(f"{len(media_source)}/{len(records)} records described a media source")
        print(f"Source Media Mimetypes: {Counter(media_src_mimetypes)}")
        print(f"Destination Media Mimetypes: {Counter(media_mimetypes)}")

    thumb_source = [
        r for r in records if r.get('thumbnail_source', r.get('is_shown_by'))]
    thumb_harvested = [r for r in records if r.get('thumbnail')]
    thumb_src_mimetypes = [
        r.get('thumbnail_source', {}).get('mimetype') for r in records]
    thumb_mimetypes = [r.get('thumbnail', {}).get('mimetype') for r in records]
    print(f"Harvested {len(thumb_harvested)} thumbnail records")
    print(f"{len(thumb_source)}/{len(records)} described a thumbnail source")
    print(f"Source Thumbnail Mimetypes: {Counter(thumb_src_mimetypes)}")
    print(f"Destination Thumbnail Mimetypes: {Counter(thumb_mimetypes)}")

    child_contents = [len(record.get('children', [])) for record in records]

    return {
        'thumb_source_mimetypes': Counter(thumb_src_mimetypes),
        'thumb_mimetypes': Counter(thumb_mimetypes),
        'media_source_mimetypes': Counter(media_src_mimetypes),
        'media_mimetypes': Counter(media_mimetypes),
        'children': sum(child_contents),
        'records': len(records)
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content using a page of mapped metadata")
    parser.add_argument('collection_id', help="Collection ID")
    parser.add_argument('mapped_page_path', help="URI-formatted path to a mapped metadata page")
    parser.add_argument('with_content_urls_version', help="URI-formatted path to a with_content_urls version")
    parser.add_argument('mapper_type', help="If 'nuxeo.nuxeo', use Nuxeo auth")
    args = parser.parse_args()

    print(harvest_page_content(
        args.collection_id,
        args.mapper_type,
        args.mapped_page_path,
        args.with_content_urls_version
    ))
