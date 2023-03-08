import json
import logging
import os
import settings
from lambda_function import harvest_page_content


# {"collection_id": 26098, "mapper_type": "nuxeo"}
def harvest_collection_content(payload, context):
    if settings.LOCAL_RUN or isinstance(payload, str):
        payload = json.loads(payload)

    collection_id = payload.get('collection_id')

    if not collection_id:
        print("ERROR ERROR ERROR")
        print('collection_id required')
        exit()

    count = 0
    page_count = 0
    print(settings.DATA_SRC)
    if settings.DATA_SRC == 'local':
        mapped_path = settings.local_path(
            'mapped_metadata', collection_id)
        try:
            page_list = [f for f in os.listdir(mapped_path)
                         if os.path.isfile(os.path.join(mapped_path, f))]
        except FileNotFoundError as e:
            logging.debug(f"{e} - have you mapped {collection_id}?")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': (
                        f"{repr(e)} - have you mapped {collection_id}? ",
                        f"looked in dir {e.filename}"
                    ),
                    'payload': payload
                })
            }
        for page in page_list:
            payload.update({'page_filename': page})
            return_val = harvest_page_content(json.dumps(payload), {})
            # print(return_val)
            # count += return_val['num_records_mapped']
            # page_count += 1
        return {
            'statusCode': 200,
            'body': {
                'collection_id': collection_id,
                # 'count': count,
            }
        }


