from .by_page import harvest_page_content
from . import settings
from rikolti.utils.versions import get_mapped_pages, create_content_data_version


def harvest_collection_content(collection_id, mapper_type, mapped_data_version: str):
    if not collection_id or not mapped_data_version:
        print("Error: collection_id and mapped_data_version required")
        exit()

    page_list = get_mapped_pages(
        mapped_data_version, **settings.AWS_CREDENTIALS)

    print(f"{collection_id:<6}: Harvesting content for {len(page_list)} pages")
    collection_stats = {
        'pages': len(page_list),
        'collection_id': collection_id
    }

    content_data_version = create_content_data_version(mapped_data_version)
    for page_path in page_list:
        page_stats = harvest_page_content(
            collection_id, page_path, content_data_version, rikolti_mapper_type=mapper_type)

        # in some cases, value is int and in some cases, value is Counter
        # so we can't just collection_stats.get(key, 0) += value
        for key, value in page_stats.items():
            if key in collection_stats:
                collection_stats[key] += value
            else:
                collection_stats[key] = value

    return collection_stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Harvest content by collection using mapped metadata")
    parser.add_argument('collection_id', help="Collection ID")
    parser.add_argument('mapped_data_version', help="URI to mapped data version: ex: s3://rikolti-data-root/3433/vernacular_data_version_1/mapped_data_version_2/")
    parser.add_argument('mapper_type', help="If 'nuxeo.nuxeo', use Nuxeo auth")
    args = parser.parse_args()
    print(harvest_collection_content(args.collection_id, args.mapper_type, args.mapped_data_version))