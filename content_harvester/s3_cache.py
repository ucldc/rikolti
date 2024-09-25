import boto3

class S3Cache(object):
    """
    Create a dictionary-like interface to an s3 bucket
    """
    def __init__(self, bucket_name, **kwargs):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3', **kwargs)
    
    def __getitem__(self, key):
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except Exception as e:
            print(f"{self} error getting {key}")
            raise e

    def get(self, key, default=None):
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except Exception:
            return default

    def __setitem__(self, key, data):
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=data)
        except Exception as e:
            print(f"{self} error setting {key}: {e}")
            return None

    def __contains__(self, key):
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except Exception:
            return False

    def __delitem__(self, key):
        self.s3.delete_object(Bucket=self.bucket_name, Key=key)

    def __repr__(self):
        return f"S3Cache(bucket_name={self.bucket_name})"
