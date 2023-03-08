import json
import os
import settings
from by_page import harvest_page_content


# {"collection_id": 26098, "rikolti_mapper_type": "nuxeo.nuxeo"}
def harvest_collection(collection):
    if isinstance(collection, str):
        collection = json.loads(collection)

    collection_id = collection.get('collection_id')

    if not collection_id:
        print("ERROR ERROR ERROR\ncollection_id required")
        exit()

    mapped_path = settings.local_path('mapped_metadata', collection_id)
    try:
        page_list = [f for f in os.listdir(mapped_path)
                        if os.path.isfile(os.path.join(mapped_path, f))]
    except FileNotFoundError as e:
        print(f"{e} - have you mapped {collection_id}?")
        return

    print(f"[{collection_id}]: Harvesting content for {len(page_list)} pages")
    collection_stats = {}
    for page in page_list:
        collection.update({'page_filename': page})
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

