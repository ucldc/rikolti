import boto3

class S3Cache(object):
    """
    Create a dictionary-like interface to an s3 bucket
    """
    def __init__(self, bucket_name, **kwargs):
        self.bucket_name = bucket_name
        try:
            self.s3 = boto3.client('s3', **kwargs)
            self.s3.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            print(f"Error connecting to s3 cache backend {bucket_name}: {e}")
            raise e
    
    def __getitem__(self, key):
        """
        Get the value for the key using bracket notation: S3Cache[key]
        If the key does not exist, raise the exception with added key context
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except Exception as e:
            print(f"{self} error getting {key}")
            raise e

    def get(self, key, default=None):
        """
        If the key exists, return the value
        If the key does not exist (defined by a NoSuchKey exception), 
        return the default
        All other exceptions should be raised with added key context
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except self.s3.exceptions.NoSuchKey:
            return default
        except Exception as e:
            print(f"{self} error getting {key}: {e}")
            raise e

    def __setitem__(self, key, data):
        """
        Set the key to the data via assignment: S3Cache[key] = data
        Raise any exceptions with added key context
        """
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=data)
        except Exception as e:
            print(f"{self} error setting {key}: {e}")
            raise e

    def __contains__(self, key):
        """
        Check if the key exists:
            True is a successful head request
            False is a 404 ClientError
        All other exceptions should be raised with added key context
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
        return f"S3Cache(bucket_name={self.bucket_name})"
