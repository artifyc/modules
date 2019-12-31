import io
import os
import os.path
import boto3
import botocore
import logging
from typing import BinaryIO, Callable, List, Union
from botocore.exceptions import ClientError


class S3Key:
    """An Amazon S3 Key"""

    def __init__(self, bucket: "S3Bucket", name: str) -> None:
        """constructor"""
        super(S3Key, self).__init__()
        self.bucket = bucket
        self.name = name

    def __repr__(self) -> str:
        """str representation of an s3key"""
        return "<S3Key name={0!r} bucket={1!r}>".format(self.name, self.bucket.name)

    def __len__(self) -> int:
        """returns the size of the s3 object of this key in bytes"""
        return self.size()

    @property
    def _boto_object(self):  # type: ignore
        """the underlying boto3 s3 key object"""
        return self.bucket._boto_s3.Object(self.bucket.name, self.name)

    def get(self) -> str:
        """Gets the value of the key."""
        return self._boto_object.get()["Body"].read()

    def download(self, file: Union[str, BinaryIO], callback: Callable = None) -> None:
        """download the key to the given path or file object"""
        if self.name not in self.bucket:
            raise Exception("this key does not exist!")

        _download = self.bucket._boto_s3.meta.client.download_fileobj
        if isinstance(file, str):
            with open(file, "wb") as data:
                _download(self.bucket.name, self.name, data, Callback=callback)
        elif isinstance(file, io.IOBase):
            _download(self.bucket.name, self.name, file, Callback=callback)

    def upload(self, file: Union[str, BinaryIO], callback: Callable = None) -> None:
        """upload the file or file obj at the given path to this key"""
        _upload = self.bucket._boto_s3.meta.client.upload_fileobj
        if isinstance(file, str):
            if not os.path.isfile(file):
                raise Exception("file does not exist!")
            with open(file, "rb") as data:
                _upload(data, self.bucket.name, self.name, Callback=callback)
        elif isinstance(file, io.IOBase):
            _upload(file, self.bucket.name, self.name, Callback=callback)

    def size(self) -> int:
        """get the size of this object in s3"""
        total = 0
        for key in self.bucket._boto_bucket.objects.filter(Prefix=self.name):
            total += key.size
        return total

    def set(self, value: str, metadata: dict = None, content_type: str = "") -> dict:
        """Sets the key to the given value."""
        if not metadata:
            metadata = {}
        return self._boto_object.put(
            Body=value, Metadata=metadata, ContentType=content_type
        )

    def rename(self, new_name: str) -> None:
        """renames the key to a given new name"""
        # copy the item to avoid pulling and pushing
        self.bucket._boto_s3.Object(self.bucket.name, new_name).copy_from(
            CopySource="{}/{}".format(self.bucket.name, self.name)
        )
        # Delete the current key.
        self.delete()
        # Set the new name.
        self.name = new_name

    def delete(self,) -> dict:
        """Deletes the key."""
        return self._boto_object.delete()

    @property
    def is_public(self) -> bool:
        """returns True if the public-read ACL is set for the Key."""
        for grant in self._boto_object.Acl().grants:
            if "AllUsers" in grant["Grantee"].get("URI", ""):
                if grant["Permission"] == "READ":
                    return True

        return False

    def make_public(self) -> dict:
        """sets the 'public-read' ACL for the key."""
        if not self.is_public:
            return self._boto_object.Acl().put(ACL="public-read")
        return {}

    @property
    def meta(self) -> dict:
        """returns the metadata for the key."""
        return self._boto_object.get()["Metadata"]

    @meta.setter
    def meta(self, value: dict) -> None:
        """sets the metadata for the key."""
        self.set(self.get(), value)

    @property
    def url(self) -> str:
        """returns the public URL for the given key."""
        if self.is_public:
            return "{0}/{1}/{2}".format(
                self.bucket._boto_s3.meta.client.meta.endpoint_url,
                self.bucket.name,
                self.name,
            )
        raise ValueError(
            "{0!r} does not have the public-read ACL set. "
            "Use the make_public() method to allow for "
            "public URL sharing.".format(self.name)
        )

    def temp_url(self, duration: int = 120) -> str:
        """returns a temporary URL for the given key."""
        return self.bucket._boto_s3.meta.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket.name, "Key": self.name},
            ExpiresIn=duration,
        )

    def create_presigned_url(self, expiration=3600):
        """Generate a presigned URL to share an S3 object
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Presigned URL as string. If error, returns None.
        """
        try: 
            response = self.bucket._boto_s3.meta.client.generate_presigned_url(
                "get_object",
                Params= {"Bucket": self.bucket.name, "Key": self.name},
                ExpiresIn=expiration,
            )
        except ClientError as e:
            logging.error(e)
            print(e)
            return None
        
        logging.info(f'Presigned URL to share an S3 object: {url}')
        return response

    def create_presigned_url_expanded(self, client_method_name, method_parameters=None,
                                  expiration=3600, http_method=None):
    """Generate a presigned URL to invoke an S3.Client method

    Not all the client methods provided in the AWS Python SDK are supported.

    :param client_method_name: Name of the S3.Client method, e.g., 'list_buckets'
    :param method_parameters: Dictionary of parameters to send to the method
    :param expiration: Time in seconds for the presigned URL to remain valid
    :param http_method: HTTP method to use (GET, etc.)
    :return: Presigned URL as string. If error, returns None.
    """
    # Generate a presigned URL for the S3 client method
    s3_client = boto3.client('s3')
    try:
        response = self.bucket._boto_s3.meta.client.generate_presigned_url(
                ClientMethod=client_method_name,                                  
                Params=method_parameters,
                ExpiresIn=expiration,
                HttpMethod=http_method
                )
    except ClientError as e:
        logging.error(e)
        return None

    logging.info(f'Presigned URL to share an S3 object: {url}')
    return response