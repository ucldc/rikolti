import os
import boto3
from .storage import DataStorage, parse_data_uri


def copy_s3_to_local(data: DataStorage, destination: str, **kwargs):
    """
    Copy the object(s) located at data.path to the local filesystem at destination
    """
    s3 = boto3.client('s3', **kwargs)
    # list all objects at data.path
    prefix = data.path.lstrip('/')
    s3_objects = s3.list_objects_v2(
        Bucket=data.bucket,
        Prefix=prefix
    )
    if 'Contents' not in s3_objects:
        raise FileNotFoundError(f"Error: {data.uri} not found")

    for obj in s3_objects['Contents']:
        target_path = os.path.join(destination, obj['Key'][len(prefix):])
        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
        s3.download_file(
            data.bucket,
            obj['Key'],
            target_path
        )

    return destination


def copy_to_local(data_uri: str, destination: str, **kwargs):
    """
    Copies the file located at data_uri to the local filesystem at destination.
    """
    data = parse_data_uri(data_uri)
    if data.store == 's3':
        return copy_s3_to_local(data, destination, **kwargs)
    else:
        raise Exception(f"Unknown data store: {data.store}")


if __name__ == "__main__":
    """
    Copy content from s3 to local filesystem, preserving directory
    structure. Overwrites files of the same name at the same location,
    but otherwise does not clean out directories prior to copying.

    Usage:
        python s3copy.py s3://rikolti-data/26147/ ../rikolti_data/26147
    """
    import argparse
    parser = argparse.ArgumentParser(
        description="Copy content from s3 to local filesystem")
    parser.add_argument(
        'data_uri',
        help="s3://bucket-name/path/to/file"
    )
    parser.add_argument(
        'destination_path',
        help="path/to/destination"
    )
    args = parser.parse_args()
    print(copy_to_local(args.data_uri, args.destination))