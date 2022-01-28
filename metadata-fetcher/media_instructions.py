import json
import os
import boto3


def validate_object(metadata):
    # print(metadata['calisphere-id'])

    if metadata['properties']['file:content'] is None:
        return False

    file_content = metadata['properties']['file:content']
    if file_content['data'] is None:
        return False
    if file_content['mime-type'] is None:
        return False
    if file_content['name'] == "empty_picture.png":
        return False
    return True


def content_filter(metadata, field, filter):
    if 'content' in metadata and metadata['content'].get(field) == filter:
        return True


def get_transcoded_video_content(metadata):
    if 'vid:transcodedVideos' not in metadata['properties']:
        return None

    transcoded = metadata['properties']['vid:transcodedVideos']
    transcoded_videos = [tv.get('content') for tv in transcoded if 
                         content_filter(tv, 'mime-type', 'video/mp4')]

    if len(transcoded_videos) <= 0:
        return None

    return transcoded_videos[0]


def make_instructions(metadata):
    # get the content file
    if metadata['type'] == 'CustomVideo':
        file_content = get_transcoded_video_content(metadata)
    else:
        file_content = metadata['properties'].get('file:content')

    file_url = file_content['data'].replace('/nuxeo/', '/Nuxeo/')
    content_file = {
        'url': file_url, 
        'mime-type': file_content['mime-type'], 
        'filename': file_content['name']        
    }

    # get the thumbnail file
    thumbnail = None
    if metadata['type'] in ['CustomVideo', 'SampleCustomPicture']:
        picture_views = metadata['properties'].get('picture:views')

        if metadata['type'] == 'CustomVideo':
            filter_val = 'StaticPlayerView'
        elif metadata['type'] == 'SampleCustomPicture':
            filter_val = 'Small'

        pv_contents = [pv.get('content') for pv in picture_views if
                       pv.get('title') == filter_val]

        if len(pv_contents) > 0:
            thumbnail_content = pv_contents[0]
            thumb_url = thumbnail_content['data'].replace('/nuxeo/', '/Nuxeo/')
            thumbnail = {
                'url': thumb_url,
                'mime-type': thumbnail_content['mime-type'],
                'filename': thumbnail_content['name']
            }

    return {
        "contentFile": content_file,
        "thumbnail": thumbnail
    }


def main(collection_id, page):
    line_number = 0

    bucket = os.environ.get('S3_BUCKET', False)
    s3_client = boto3.client('s3')
    response = s3_client.get_object(
        Bucket=bucket,
        Key=f"vernacular_metadata/{collection_id}/{page}.jsonl"
    )
    child_folders = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f'vernacular_metadata/{collection_id}/children/')
    try:
        child_folders = [child['Key'] for child in child_folders['Contents']]
    except KeyError:
        child_folders = []

    # path = os.path.join(os.getcwd(), collection_id)
    # filename = os.path.join(path, f"{page}.jsonl")
    # file = open(filename)
    # child_folders = os.listdir(os.path.join(path, "children"))

    # for line in file:
    for line in response['Body'].iter_lines():
        media_instructions = {}
        record = json.loads(line)

        if validate_object(record):
            calisphere_id = record['calisphere-id']
        else: 
            continue

        media_instructions.update({
            calisphere_id: make_instructions(record)
        })

        calisphere_id = record['calisphere-id']
        uuid = calisphere_id[len(collection_id)+2:]
        child_prefix = (
            f"vernacular_metadata/{collection_id}/"
            f"children/{uuid}/")

        # if uuid in child_folders:
        if f"{child_prefix}0.jsonl" in child_folders:
            children = []
            pages = [key for key in child_folders 
                     if key.startswith(child_prefix)]
            # pages = os.listdir(os.path.join(path, "children", uuid))
            pages.sort()
            for page in pages:
                # page_path = os.path.join(path, "children", uuid, page)
                # child_page = open(page_path)
                child_page = s3_client.get_object(
                    Bucket=bucket,
                    Key=page)
                # for line in child_page:
                for line in child_page['Body'].iter_lines():
                    record = json.loads(line)
                    if validate_object(record):
                        #record_id = record['uid']
                        child_calisphere_id = f"{collection_id}--{record['uid']}"
                        children.append(dict({"calisphere-id": child_calisphere_id},
                                             **make_instructions(record)))
                # child_page.close()
            media_instructions[calisphere_id].update({"children": children})

        jsonl = [dict({"calisphere-id": k}, **v)
                 for k, v in media_instructions.items()][0]

        s3_client.put_object(
            Bucket=bucket,
            Key=f"media_instructions/{collection_id}-test/{uuid}.json",
            Body=json.dumps(jsonl))


if __name__ == "__main__":
    main('76', 0)
