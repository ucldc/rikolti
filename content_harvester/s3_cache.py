import boto3
from typing import Optional

class S3Cache(object):
    """
    Create a dictionary-like interface to an s3 bucket

    Parameters:
        bucket_name (str - required): The name of the s3 bucket
        prefix (str - optional): A string to prepend to all keys
            include any necessary slashed when specifying the prefix
        **kwargs: Passed directly to the boto3 client
    """
    def __init__(self, bucket_name: str, prefix: Optional[str]=None, **kwargs):
        self.bucket_name = bucket_name
        self.prefix = prefix or ''
        try:
            self.s3 = boto3.client('s3', **kwargs)
            self.s3.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            print(f"Error connecting to s3 cache backend {bucket_name}: {e}")
            raise e
    
    def __getitem__(self, key):
        """
        Get the value for the key using bracket notation: S3Cache[key]
        Raise all exceptions with added key context
        """
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name, Key=self.prefix+key)
            return response['Body'].read()
        except Exception as e:
            print(f"{self} error getting {key}")
            raise e

    def get(self, key, default=None):
        """
        Get the value for the key or, if S3 raises NoSuchKey, return the
        default; raise all other exceptions with added key context

        Parameters:
            key (str - required): The key to retrieve from the cache
            default (any - optional): The value to return if the key
            does not exist [default: None]
        """
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name, Key=self.prefix + key)
            return response['Body'].read()
        except self.s3.exceptions.NoSuchKey:
            return default
        except Exception as e:
            print(f"{self} error getting {key}: {e}")
            raise e

    def __setitem__(self, key, data):
        """
        Set the key to the data via assignment: S3Cache[key] = data
        Raise all exceptions with added key context
        """
        try:
            self.s3.put_object(
                Bucket=self.bucket_name, Key=self.prefix + key, Body=data)
        except Exception as e:
            print(f"{self} error setting {key}: {e}")
            raise e

    def __contains__(self, key):
        """
        Check if the key exists in the cache using the 'in' operator:
            True is a successful S3 head request
            False is a 404 ClientError
        Raise all other exceptions with added key context
        """
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(f"{self} error checking {key}: {e}")
                raise e
        except Exception as e:
            print(f"{self} error checking {key}: {e}")
            raise e

    def __delitem__(self, key):
        self.s3.delete_object(Bucket=self.bucket_name, Key=key)

    def __repr__(self):
        readable = f"S3Cache(bucket_name={self.bucket_name}"
        if self.prefix:
            return f"{readable}, prefix={self.prefix})"
        return f"{readable})"
