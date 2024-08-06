import asyncio
import hashlib
import os
from datetime import datetime
import aiofiles
from metaDataManeger import MetadataManager
from metadata import MetadataManager

URL_SERVER = 'D:\\בוטקמפ\\server'
class S3ClientSimulator:
    def __init__(self, metadata_file):
        self.metadata_manager = MetadataManager(metadata_file)

    async def get_object_lock_configuration(self, bucket):
        # Check if the bucket exists
        bucket_metadata = self.metadata_manager.metadata["server"]["buckets"].get(bucket)

        if bucket_metadata is None:
            raise FileNotFoundError(f"Bucket '{bucket}' not found.")

        # Retrieve the object lock configuration for the bucket
        object_lock = bucket_metadata.get("objectLock", None)
        object_lock_configuration = {
            "ObjectLockEnabled": object_lock["objectLockEnabled"] if object_lock else "DISABLED",
            "LockConfiguration": object_lock["lockConfiguration"] if object_lock else {}
        }

        return {
            "ObjectLockConfiguration": object_lock_configuration
        }


    async def get_object_torrent(self, bucket, key, version_id=None, is_sync=True, IfMatch=None,if_modified_since=None,if_none_match=None,
        if_unmodified_since=None,range=None,ssec_ustomer_algorithm=None,ssec_ustomer_key=None,ssec_ustomerkey_md5=None,request_payer=None,):

        # Retrieve the object metadata
        metadata = self.metadata_manager.get_bucket_metadata(bucket, key)

        if not metadata:
            raise FileNotFoundError(f"Object {key} not found in bucket {bucket}")

        # If version_id is provided, fetch that specific version
        if version_id:
            version_metadata = metadata.get('versions', {}).get(version_id)
            if not version_metadata:
                raise FileNotFoundError(f"Version {version_id} not found for object {key} in bucket {bucket}")
        else:
            # If no version_id is provided, get the latest version
            version_id = self.metadata_manager.get_latest_version(bucket, key)
            version_metadata = metadata.get('versions', {}).get(version_id)

        # Prepare the torrent information (this is a placeholder, modify as needed)
        torrent_info = {
            "bucket": bucket,
            "key": key,
            "version_id": version_id,
            "etag": version_metadata.get('etag'),
            "size": version_metadata.get('size'),
            "last_modified": version_metadata.get('lastModified'),
            "content_type": version_metadata.get('contentType'),
            "metadata": version_metadata.get('metadata', {})
        }

        return torrent_info

    async def head_object(self, bucket, key, version_id=None,is_async=True,IfMatch=None, IfModifiedSince=None,IfNoneMatch=None,IfUnmodifiedSince=None,
    Range=None,VersionId=None,SSECustomerAlgorithm=None,SSECustomerKey=None,SSECustomerKeyMD5=None,RequestPayer=None):

        # Retrieve the object metadata
        metadata = self.metadata_manager.get_bucket_metadata(bucket, key)

        print(f"Retrieved metadata for bucket '{bucket}' and key '{key}': {metadata}")

        if not metadata:
            raise FileNotFoundError(f"Object {key} not found in bucket {bucket}")

        # If version_id is provided, fetch that specific version
        if version_id:
            version_metadata = metadata.get('versions', {}).get(version_id)
            if not version_metadata:
                raise FileNotFoundError(f"Version {version_id} not found for object {key} in bucket {bucket}")
        else:
            # If no version_id is provided, get the latest version
            version_id = self.metadata_manager.get_latest_version(bucket, key)
            print(f"Latest version ID for object '{key}': {version_id}")
            version_metadata = metadata.get('versions', {}).get(version_id)

        print(f"Version metadata: {version_metadata}")

        if not version_metadata:
            raise FileNotFoundError(f"No version metadata found for object {key} with version {version_id}")

        # Prepare the response metadata
        response_metadata = {
            "ContentLength": version_metadata.get('size'),
            "LastModified": version_metadata.get('lastModified'),
            "ContentType": version_metadata.get('contentType'),
            "ETag": version_metadata.get('etag'),
            "Metadata": version_metadata.get('metadata', {}),
            "VersionId": version_id,
            "ObjectLock": metadata.get('objectLock', {})
        }

        return response_metadata
    async def put_object(self, bucket, key, body, acl=None, metadata=None,
    content_type=None, sse_customer_algorithm=None,sse_customer_key=None, sse_customer_key_md5=None):

        # Check if the bucket exists
        bucket_metadata = self.metadata_manager.metadata["server"]["buckets"].get(bucket)

        if not bucket_metadata:
            bucket_metadata = {"objects": {}}
            self.metadata_manager.metadata["server"]["buckets"][bucket] = bucket_metadata

        object_metadata = bucket_metadata["objects"].get(key, {"versions": {}})

        # Determine the new version ID
        version_id = str(len(object_metadata["versions"]) + 1)  # Simple versioning

        # Create the file path with the new version ID before the extension
        file_name, file_extension = os.path.splitext(key)
        versioned_file_name = f"{file_name}.v{version_id}{file_extension}"
        file_path = os.path.join(URL_SERVER, bucket, versioned_file_name)

        # Create the directory structure if it doesn't exist
        if '/' in key:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Write the body to the file
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(body)

        # Update previous versions' isLatest to False
        for version in object_metadata["versions"].values():
            version["isLatest"] = False

        # Prepare the metadata for the new version
        etag = self.generate_etag(body)
        object_metadata["versions"][version_id] = {
            "etag": etag,
            "size": len(body),
            "lastModified": datetime.utcnow().isoformat() + "Z",
            "isLatest": True,
            "acl": acl if acl else {"owner": "default_owner", "permissions": ["READ", "WRITE"]},
            "legalHold": {"Status": "OFF"},
            "retention": {"mode": "NONE"},
            "tagSet": [],
            "contentLength": len(body),
            "contentType": content_type if content_type else "application/octet-stream",
            "metadata": metadata if metadata else {}
        }

        bucket_metadata["objects"][key] = object_metadata

        # Save metadata to file
        await self.metadata_manager.save_metadata()

        return {
            "ETag": object_metadata["versions"][version_id]["etag"],
            "VersionId": version_id
        }
    async def put_object_acl(self, bucket, key, acl, version_id=None,is_sync=True,GrantFullControl=None,GrantRead=None,GrantReadACP=None,GrantWriteACP=None):
        # Check if the bucket exists
        bucket_metadata = self.metadata_manager.metadata["server"]["buckets"].get(bucket)
        if not bucket_metadata:
            raise FileNotFoundError(f"Bucket '{bucket}' does not exist")

        object_metadata = bucket_metadata["objects"].get(key)

        if not object_metadata:
            raise FileNotFoundError(f"Object '{key}' does not exist in bucket '{bucket}'")

        # If version_id is not provided, find the latest version
        if not version_id:
            for vid, metadata in object_metadata["versions"].items():
                if metadata["isLatest"]:
                    version_id = vid
                    break

        if not version_id or version_id not in object_metadata["versions"]:
            raise FileNotFoundError(f"Version '{version_id}' does not exist for object '{key}' in bucket '{bucket}'")

        # Update the ACL for the specified version
        object_metadata["versions"][version_id]["acl"] = acl

        # Save metadata to file
        await self.metadata_manager.save_metadata(is_sync)

        return {
            "VersionId": version_id,
            "ACL": acl
        }
    def generate_etag(self, content):
        return hashlib.md5(content).hexdigest()
# Example usage:
async def main():
    s3_client = S3ClientSimulator(f'{URL_SERVER}\\metadata.json')
    try:
        lock_config = await s3_client.get_object_lock_configuration('bucket1')
        print(lock_config)
    except FileNotFoundError as e:
        print(e)
    try:
        torrent_info = await s3_client.get_object_torrent('bucket1', 'file.txt')
        print("Torrent Info for latest version:", torrent_info)
    except FileNotFoundError as e:
        print(e)

    # Example of getting specific version torrent info
    try:
        torrent_info = await s3_client.get_object_torrent('bucket1', 'file.txt', version_id='1')
        print("Torrent Info for version 1:", torrent_info)
    except FileNotFoundError as e:
        print(e)
    head_object = await s3_client.head_object('bucket1', 'file2.txt')
    print("meta data is:", head_object)
    # try:
    #     # Example body as bytes
    #     body = b"Hello, World!"
    #     result = await s3_client.put_object('bucket1', 'file3.txt', body)
    #     print("PutObject result:", result)
    # except Exception as e:
    #     print(e)
    # try:
    #     response = await s3_client.put_object_acl('bucket1', 'file2.txt', {"owner": "default_owner", "permissions": ["READ", "WRITE"]},version_id="3")
    #     print(response)
    # except Exception as e:
    #     print(e)

if __name__ == "__main__":
    asyncio.run(main())







# async def get_object_torrent(self, bucket, key, version_id=None, is_sync=True):
#     # Retrieve the object metadata
#     metadata = self.metadata_manager.get_bucket_metadata(bucket, key)
#
#     if not metadata:
#         raise FileNotFoundError(f"Object {key} not found in bucket {bucket}")
#
#     # If version_id is provided, fetch that specific version
#     if version_id:
#         version_metadata = metadata.get('versions', {}).get(version_id)
#         if not version_metadata:
#             raise FileNotFoundError(f"Version {version_id} not found for object {key} in bucket {bucket}")
#     else:
#         # If no version_id is provided, get the latest version
#         version_id = self.metadata_manager.get_latest_version(bucket, key)
#         version_metadata = metadata.get('versions', {}).get(version_id)
#
#     # Prepare the torrent information
#     file_path = f'D:/בוטקמפ/server/{bucket}/{key}'
#     torrent_path = f'D:/בוטקמפ/server/torrents/{bucket}/{key}.torrent'
#
#     if not os.path.exists(os.path.dirname(torrent_path)):
#         os.makedirs(os.path.dirname(torrent_path))
#
#     piece_length = 512 * 1024  # 512KB
#     pieces = []
#     with open(file_path, 'r') as f:
#         while True:
#             piece = f.read(piece_length)
#             if not piece:
#                 break
#             pieces.append(hashlib.sha1(piece).digest())
#
#     info = {
#         'name': key,
#         'piece length': piece_length,
#         'pieces': b''.join(pieces),
#         'length': os.path.getsize(file_path)
#     }
#
#     torrent = {
#         'announce': 'http://tracker.example.com/announce',
#         'info': info
#     }
#
#     with open(torrent_path, 'w') as f:
#         f.write(bencodepy.encode(torrent))
#
#     # Return the torrent file path
#     return torrent_path

class PyStorage:

    def __init__(self):
        # Initialize the metadata manager
        self.metadata_manager = MetadataManager()

    def sync_get_object_attributes(self, file_path):
        # Synchronously get object attributes by running the asynchronous function
        attributes = asyncio.run(self.get_object_attributes(file_path))
        return attributes

    async def get_object_attributes(self, key, version_id=None, async_flag=False):  
        # Get the metadata for the given key
        if async_flag:
            metadata = await asyncio.to_thread(self.metadata_manager.get_metadata, key)
        else:
            metadata = self.metadata_manager.get_metadata(key)   
              
        if metadata is None:
            raise FileNotFoundError(f'No metadata found for object {key}')

        # Determine which version to use
        if version_id is None:
            # If no version is specified, use the latest version
            version_id = max(metadata['versions'].keys(), key=int)

        # Get the metadata for the specified version
        version_metadata = metadata['versions'].get(str(version_id))

        if version_metadata is None:
            raise FileNotFoundError(f'No version found with ID {version_id} for object {key}')

        # Extract the required information for the response
        attributes = {
            'checksum': version_metadata.get('checksum'),
            'ETag': version_metadata.get('ETag'),
            'ObjectParts': version_metadata.get('ObjectParts'),
            'ObjectSize': version_metadata.get('ObjectSize'),
            'StorageClass': version_metadata.get('StorageClass', {})
        }

        return attributes

    def sync_put_object_tagging(self, file_path, tags, version_id=None):
        # Synchronously put object tagging by running the asynchronous function
        asyncio.run(self.put_object_tagging(file_path, tags, version_id))

    async def put_object_tagging(self, file_path, tags, version_id=None, async_flag=False):
        # Check if async_flag is a boolean
        if not isinstance(async_flag, bool):
            raise TypeError('async_flag must be a boolean')

        # Update metadata tags asynchronously or synchronously based on async_flag
        if async_flag:
            await self.metadata_manager.update_metadata_tags(file_path, version_id, {'TagSet': tags}, False)
        else:
            await self.metadata_manager.update_metadata_tags(file_path, version_id, {'TagSet': tags}, True)

        print(f'Tags for {file_path} have been saved.')

    def sync_get_object_tagging(self, file_path, version_id=None):
        # Synchronously get object tagging by running the asynchronous function
        tags = asyncio.run(self.get_object_tagging(file_path, version_id))
        return tags

    async def get_object_tagging(self, file_path, version_id=None, async_flag=False):
        # Check if async_flag is a boolean
        if not isinstance(async_flag, bool):
            raise TypeError('async_flag must be a boolean')

        # Get tags asynchronously or synchronously based on async_flag
        if async_flag:            
            tags = await asyncio.to_thread(self.metadata_manager.get_tags, file_path, version_id)
        else:
            tags = self.metadata_manager.get_tags(file_path, version_id)
        return tags
