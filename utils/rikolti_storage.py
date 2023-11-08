import os
import re

import boto3

from urllib.parse import urlparse
from typing import Optional

class RikoltiStorage():
    def __init__(self, data_url: str, **kwargs):
        self.data_url = data_url
        data_loc = urlparse(data_url)
        self.data_store = data_loc.scheme
        self.data_bucket = data_loc.netloc
        self.data_path = data_loc.path

        if self.data_store == 's3':
            self.s3 = boto3.client('s3', **kwargs)

    def list_pages(self, recursive=True, relative=True) -> list:
        if self.data_store == 's3':
            try:
                return self.list_s3_pages(recursive=recursive, relative=relative)
            except Exception as e:
                url = (
                    f"https://{self.data_bucket}.s3.us-west-2.amazonaws"
                    ".com/index.html#{self.data_path}/"
                )
                raise Exception(
                    f"Error listing files at {self.data_url}\n"
                    f"Check that {self.data_path} exists at {url}\n{e}"
            )
        elif self.data_store == 'file':
            try:
                return self.list_file_pages(recursive=recursive, relative=relative)
            except Exception as e:
                raise Exception(f"Error listing files in {path}\n{e}")
        else:
            raise Exception(f"Unknown data store: {self.data_store}")

    def list_s3_pages(self, recursive=True, relative=True) -> list:
        """
        List all objects in s3_bucket with prefix s3_prefix
        """
        s3_objects = self.s3.list_objects_v2(
            Bucket=self.data_bucket, 
            Prefix=self.data_path
        )
        # TODO: check resp['IsTruncated'] and use ContinuationToken if needed

        keys = [f"s3://{self.data_bucket}/{obj['Key']}" for obj in s3_objects['Contents']]
        prefix = "s3://{self.data_bucket}/{self.data_path}"

        if not recursive:
            # prune deeper branches
            leaf_regex = re.escape(prefix) + r"^\/?[\w!'_.*()-]+\/?$"
            keys = [key for key in keys if re.match(leaf_regex, key)]

        if relative:
            keys = [key[len(prefix):] for key in keys]

        return keys

    def list_file_pages(self, recursive=True, relative=True) -> list:
        """
        List all files in file_path
        """
        file_objects = []
        if recursive:
            for root, dirs, files in os.walk(self.data_path):
                root_uri = "file://{root}/" if root[-1] != '/' else "file://{root}"
                for file in files:
                    file_objects.append(f"{root_uri}{file}")

        if not recursive:
            for file in os.listdir(self.data_path):
                if os.path.isfile(os.path.join(self.data_path, file)):
                    root_uri = "file://{self.data_path}/" if self.data_path[-1] != '/' else "file://{self.data_path}"
                    file_objects.append(f"{root_uri}{file}")

        if relative:
            prefix = "file://{self.data_path}/"
            file_objects = [file[len(prefix):] for file in file_objects]

        return file_objects

    def search_page(self, search_str: str, page: str) -> bool:
        if self.data_store == 's3':
            return self.search_s3_page(search_str, page)
        elif self.data_store == 'file':
            return self.search_file_page(search_str, page)
        else:
            raise Exception(f"Unknown data store: {self.data_store}")

    def search_s3_page(self, search_str: str, s3_key: str) -> bool:
        """
        Check if search_str is in the body of the object located at s3_key
        Returns the s3_key of the object if so, otherwise returns None
        """
        obj = self.s3.get_object(Bucket=self.data_bucket, Key=s3_key)
        body = obj['Body'].read().decode('utf-8')
        if search_str in body:
            return True
        else:
            return False

    def search_file_page(self, search_str: str, file_path: str) -> bool:
        """
        Check if search_str is in the body of the file located at file_path
        """
        with open(file_path, 'r') as f:
            body = f.read()
            if search_str in body:
                return True
            else:
                return False

    def get_page_content(self):
        if self.data_store == 's3':
            return self.get_s3_contents()
        elif self.data_store == 'file':
            return self.get_file_contents()
        else:
            raise Exception(f"Unknown data store: {self.data_store}")

    def get_s3_contents(self):
        """
        Get the body of the object located at s3_key
        """
        try:
            obj = self.s3.get_object(Bucket=self.data_bucket, Key=self.data_path)
            return obj['Body'].read().decode('utf-8')
        except Exception as e:
            url = (
                f"https://{self.data_bucket}.s3.us-west-2.amazonaws.com/"
                "index.html#{self.data_path}/"
            )
            raise Exception(
                f"Error reading file at {self.data_url}\nCheck: {url}\n{e}"
            )
    
    def get_file_contents(self):
        """
        Get the body of the file located at file_path
        """
        try:
            with open(self.data_path, 'r') as f:
                return f.read()
        except Exception as e:
            raise Exception(f"Error reading {self.data_path}\n{e}")


    def put_page_content(self, content:str, relative_path: Optional[str]=None):
        """
        Write content to a file at relative_path (relative to data_path).
        relative_path is a list of strings, each string is a directory name 
        representing a directory tree.
        handle s3 or file storage, use '/' as separator for s3 key and os.sep
        as separtors for file storage
        """
        path = self.data_path
        if relative_path:
            path += relative_path

        if self.data_store == 's3':
            return self.put_s3_content(path, content)
        elif self.data_store == 'file':
            return self.put_file_content(path, content)
        else:
            raise Exception(f"Unknown data store: {self.data_store}")

    def put_file_content(self, file_path, content):
        """
        Write content to a file at file_path
        """
        file_path = os.sep.join(file_path.split('/'))
        directory_path = os.path.dirname(file_path)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        with open(file_path, 'w') as f:
            f.write(content)
    
    def put_s3_content(self, s3_key, content):
        """
        Write content to an object named s3_key
        """
        self.s3.put_object(
            ACL='bucket-owner-full-control',
            Bucket=self.data_bucket,
            Key=s3_key,
            Body=content
        )

