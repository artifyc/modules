import io
import os
import os.path
import boto3
import botocore
from typing import BinaryIO, Callable, List, Union


class S3Bucket:
    """An Amazon S3 Bucket."""

    def __init__(self, name: str, create: bool = False, region: str = "") -> None:
        super(S3Bucket, self).__init__()
        self.name = name
        self.region = region or os.getenv("AWS_DEFAULT_REGION", AWS_DEFAULT_REGION)
        self._boto_s3 = boto3.resource("s3", self.region)
        self._boto_bucket = self._boto_s3.Bucket(self.name)

        # Check if the bucket exists.
        if not self._boto_s3.Bucket(self.name) in self._boto_s3.buckets.all():
            if create:
                # Create the bucket.
                self._boto_s3.create_bucket(Bucket=self.name)
            else:
                raise ValueError("The bucket {0!r} doesn't exist!".format(self.name))

    def __getitem__(self, key: str) -> str:
        """allows for accessing keys with the array syntax"""
        return self.get(key)

    def __setitem__(self, key: str, value: str) -> dict:
        """allows for setting/uploading keys with the array syntax"""
        return self.set(key, value)

    def __delitem__(self, key: str) -> dict:
        """allow for deletion of keys via the del operator"""
        return self.delete(key)

    def __contains__(self, item: str) -> bool:
        """allows for use of the in keyword on the bucket object"""
        try:
            self._boto_s3.Object(self.name, item).load()
            return True
        except botocore.exceptions.ClientError as exception:
            if exception.response["Error"]["Code"] == "404":
                # The object does not exist.
                return False
            raise  # pragma: no cover

    def list(self) -> List:
        """returns a list of keys in the bucket."""
        return [k.key for k in self._boto_bucket.objects.all()]

    @property
    def is_public(self) -> bool:
        """returns True if the public-read ACL is set for the bucket."""
        for grant in self._boto_bucket.Acl().grants:
            if "AllUsers" in grant["Grantee"].get("URI", ""):
                if grant["Permission"] == "READ":
                    return True

        return False

    def make_public(self) -> dict:
        """Makes the bucket public-readable."""
        return self._boto_bucket.Acl().put(ACL="public-read")

    def key(self, key: str) -> S3Key:
        """returns a given key from the bucket."""
        return S3Key(self, key)

    def all(self) -> List[S3Key]:
        """returns all keys in the bucket."""
        return [self.key(k) for k in self.list()]

    def get(self, key: str) -> str:
        """get the contents of the given key"""
        selected_key = self.key(key)
        return selected_key.get()

    def set(
        self, key: str, value: str, metadata: dict = None, content_type: str = ""
    ) -> dict:
        """creates/edits a key in the s3 bucket"""
        if not metadata:
            metadata = {}
        new_key = self.key(key)
        return new_key.set(value, metadata, content_type)

    def delete(self, key: str = None) -> dict:
        """Deletes the given key, or the whole bucket."""

        # Delete the whole bucket.
        if key is None:
            # Delete everything in the bucket.
            for each_key in self.all():
                each_key.delete()

            # Delete the bucket.
            return self._boto_bucket.delete()

        # If a key was passed, delete they key.
        k = self.key(key)
        return k.delete()

    def __repr__(self) -> str:
        """representation of an s3bucket object"""
        return "<S3Bucket name={0!r}>".format(self.name)



def list() -> List[str]:  # pylint: disable=redefined-builtin
    """lists buckets, by name."""
    s3_resource = boto3.resource("s3")
    return [bucket.name for bucket in s3_resource.buckets.all()]


def get(bucket_name: str, create: bool = False) -> S3Bucket:
    """get an s3bucket object by name"""
    return S3Bucket(bucket_name, create=create)