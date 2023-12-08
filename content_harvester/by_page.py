import json
import os
from collections import Counter

from . import settings
from .by_record import configure_http_session, harvest_record

from rikolti.utils.versions import (
    get_mapped_page_content, put_content_data_page
)


# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo", "page_filename": "file:///rikolti_data/r-0"}
def harvest_page_content(collection_id, mapped_page_path, content_data_version, **kwargs):
    rikolti_mapper_type = kwargs.get('rikolti_mapper_type')
    page_filename = os.path.basename(mapped_page_path)

    # Weird how we have to use username/pass to hit this endpoint
    # but we have to use auth token to hit API endpoint
    auth = None
    if rikolti_mapper_type == 'nuxeo.nuxeo':
        auth = (settings.NUXEO_USER, settings.NUXEO_PASS)
    http_session = configure_http_session()

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
            record_with_content = harvest_record(
                record, 
                collection_id, 
                page_filename, 
                mapped_page_path, 
                http_session, 
                auth
            )
            # put_content_data_page(
            #     json.dumps(record_with_content), 
            #     record_with_content.get('calisphere-id').replace(os.sep, '_') + ".json",
            #     content_data_version
            # )
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

    put_content_data_page(
        json.dumps(records), page_filename, content_data_version)

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
    parser.add_argument('content_data_version', help="URI-formatted path to a content data version")
    parser.add_argument('--nuxeo', action="store_true", help="Use Nuxeo auth")
    args = parser.parse_args()
    arguments = {
        'collection_id': args.collection_id,
        'mapped_page_path': args.mapped_page_path,
        'content_data_version': args.content_data_version
    }
    if args.nuxeo:
        arguments['rikolti_mapper_type'] = 'nuxeo.nuxeo'
    print(harvest_page_content(**arguments))
