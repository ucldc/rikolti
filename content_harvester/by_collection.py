import json

from .by_page import harvest_page_content
from . import settings
from rikolti.utils.versions import get_mapped_pages, create_content_data_version


# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo"}
def harvest_collection(collection, mapped_data_version: str):
    if isinstance(collection, str):
        collection = json.loads(collection)

    collection_id = collection.get('collection_id')

    if not collection_id or not mapped_data_version:
        print("Error: collection_id and mapped_data_version required")
        exit()

    page_list = get_mapped_pages(
        mapped_data_version,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        aws_session_token=settings.AWS_SESSION_TOKEN,
        region_name=settings.AWS_REGION
    )

    print(f"{collection_id:<6}: Harvesting content for {len(page_list)} pages")
    collection_stats = {}

    collection.update({
        'content_data_version': create_content_data_version(mapped_data_version)
    })

    for page_path in page_list:
        collection.update({'mapped_page_path': page_path})
        page_stats = harvest_page_content(**collection)

        # in some cases, value is int and in some cases, value is Counter
        # so we can't just collection_stats.get(key, 0) += value
        for key, value in page_stats.items():
            if key in collection_stats:
                collection_stats[key] += value
            else:
                collection_stats[key] = value

    collection_stats.update({
        'pages': len(page_list), 
        'collection_id': collection_id,
    })
    return collection_stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content by collection using mapped metadata")
    parser.add_argument('collection_id', help="Collection ID")
    parser.add_argument('mapped_data_version', help="URI to mapped data version: ex: s3://rikolti-data-root/3433/vernacular_data_version_1/mapped_data_version_2/")
    parser.add_argument('--nuxeo', action="store_true", help="Use Nuxeo auth")
    args = parser.parse_args()
    arguments = {
        'collection_id': args.collection_id,
    }
    if args.nuxeo:
        arguments['rikolti_mapper_type'] = 'nuxeo.nuxeo'
    print(harvest_collection(arguments, args.mapped_data_version))