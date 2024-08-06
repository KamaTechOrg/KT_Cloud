import asyncio
from metadata import MetadataManager
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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
    async def put_object_legal_hold(self, bucket, key, legal_hold_status, version_id=None, is_sync=True):
        try:
            if legal_hold_status not in ['ON', 'OFF']:
                raise ValueError("Legal hold status must be either 'ON' or 'OFF'")
            
            if not isinstance(bucket, str) or not bucket:
                raise ValueError("Bucket name must be a non-empty string")
            if not isinstance(key, str) or not key:
                raise ValueError("Object key must be a non-empty string")
            
            metadata = self.metadata_manager.get_key_metadata(bucket, key)
            if not metadata:
                raise KeyError(f"Object key '{key}' not found in metadata")
            
            if "versions" not in metadata:
                metadata["versions"] = {}
            
            if version_id is None:
                version_id = self.metadata_manager.get_latest_version(bucket, key)
            
            if version_id not in metadata["versions"]:
                metadata["versions"][version_id] = {}
            
            if "LegalHold" not in metadata["versions"][version_id]:
                metadata["versions"][version_id]["LegalHold"] = {}
            metadata["versions"][version_id]["LegalHold"]["Status"] = legal_hold_status
            
            # Save the updated metadata based on sync/asynchronous mode
            await self.metadata_manager.save_metadata(is_sync)

            return {"LegalHold": {"Status": legal_hold_status}}

        except ValueError as e:
            return {'Error': f'Invalid value: {str(e)}'}
        except KeyError as e:
            return {'Error': f'Metadata issue: {str(e)}'}
        except Exception as e:
            return {'Error': f'Unexpected error: {str(e)}'}

    async def get_object_legal_hold(self, bucket, key, version_id=None, is_async=True):
        try:
            if not isinstance(bucket, str) or not bucket:
                raise ValueError("Bucket name must be a non-empty string")
            if not isinstance(key, str) or not key:
                raise ValueError("Object key must be a non-empty string")
            
            metadata = self.metadata_manager.get_key_metadata(bucket, key)
            if not metadata:
                raise KeyError(f"Object key '{key}' not found in metadata")
            
            if "versions" not in metadata:
                raise KeyError(f"Versions not found for object key '{key}' in metadata")
            
            if version_id is None:
                version_id = self.metadata_manager.get_latest_version(bucket, key)
            
            if version_id not in metadata["versions"]:
                raise KeyError(f"Version id '{version_id}' not found for object key '{key}' in metadata")

            legal_hold = metadata["versions"][version_id].get("LegalHold", {"Status": "OFF"})
            return {"LegalHold": legal_hold}
        
        except ValueError as e:
            return {'Error': f'Invalid value: {str(e)}'}
        except KeyError as e:
            return {'Error': f'Metadata issue: {str(e)}'}
        except Exception as e:
            return {'Error': f'Unexpected error: {str(e)}'}

    async def get_object_retention(self, bucket, key, version_id=None, is_sync=True):
        try:
            if not isinstance(bucket, str) or not bucket:
                raise ValueError("Bucket name must be a non-empty string")
            if not isinstance(key, str) or not key:
                raise ValueError("Object key must be a non-empty string")
            
            metadata = self.metadata_manager.get_key_metadata(bucket, key)
            if not metadata:
                raise KeyError(f"Object key '{key}' not found in metadata")
            
            if "versions" not in metadata:
                raise KeyError(f"Versions not found for object key '{key}' in metadata")
            
            if version_id is None:
                version_id = self.metadata_manager.get_latest_version(bucket, key)
            
            if version_id not in metadata["versions"]:
                raise KeyError(f"Version id '{version_id}' not found for object key '{key}' in metadata")

            retention = metadata["versions"][version_id].get("Retention", {"Mode": "GOVERNANCE"})
            return {"Retention": retention}
        
        except ValueError as e:
            return {'Error': f'Invalid value: {str(e)}'}
        except KeyError as e:
            return {'Error': f'Metadata issue: {str(e)}'}
        except Exception as e:
            return {'Error': f'Unexpected error: {str(e)}'}

    async def put_object_retention(self, bucket, key, retention_mode, retain_until_date, version_id=None, is_sync=True):
        try:
            if retention_mode not in ['GOVERNANCE', 'COMPLIANCE']:
                raise ValueError("Retention mode must be either 'GOVERNANCE' or 'COMPLIANCE'")
            
            try:
                datetime.strptime(retain_until_date, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                raise ValueError("Retain until date must be a valid date in the format YYYY-MM-DDTHH:MM:SSZ")
            
            if not isinstance(bucket, str) or not bucket:
                raise ValueError("Bucket name must be a non-empty string")
            if not isinstance(key, str) or not key:
                raise ValueError("Object key must be a non-empty string")
            
            metadata = self.metadata_manager.get_key_metadata(bucket, key)
            if not metadata:
                raise KeyError(f"Object key '{key}' not found in metadata")
            
            if "versions" not in metadata:
                metadata["versions"] = {}
            
            if version_id is None:
                version_id = self.metadata_manager.get_latest_version(bucket, key)
            
            if version_id not in metadata["versions"]:
                metadata["versions"][version_id] = {}
            
            if "Retention" not in metadata["versions"][version_id]:
                metadata["versions"][version_id]["Retention"] = {}
            metadata["versions"][version_id]["Retention"]["Mode"] = retention_mode
            metadata["versions"][version_id]["Retention"]["RetainUntilDate"] = retain_until_date

            # Save the updated metadata based on sync/asynchronous mode
            await self.metadata_manager.save_metadata(is_sync)

            return {"Retention": {"Mode": retention_mode, "RetainUntilDate": retain_until_date}}
        
        except ValueError as e:
            return {'Error': f'Invalid value: {str(e)}'}
        except KeyError as e:
            return {'Error': f'Metadata issue: {str(e)}'}
        except Exception as e:
            return {'Error': f'Unexpected error: {str(e)}'}

    async def put_object_lock_configuration(self, bucket, object_lock_enabled, mode="GOVERNANCE", days=30, years=0, request_payer=None, token=None, ContentMD5=None, ChecksumAlgorithm=None, ExpectedBucketOwner=None, is_sync=True):
        try:
            if mode not in ['GOVERNANCE', 'COMPLIANCE']:
                raise ValueError("Retention mode must be either 'GOVERNANCE' or 'COMPLIANCE'")
            
            if not isinstance(bucket, str) or not bucket:
                raise ValueError("Bucket name must be a non-empty string")
            if object_lock_enabled not in ['Enabled', 'Disabled']:
                raise ValueError("Object lock enabled must be either 'Enabled' or 'Disabled'")
            if days < 0:
                raise ValueError("Days must be a positive integer")
            if years < 0:
                raise ValueError("Years must be a positive integer")
            
            object_lock_config = {'ObjectLockEnabled': object_lock_enabled}
            if mode:
                retention = {'Mode': mode}
                
                # Calculate the retention date
                date = datetime.utcnow()
                new_date = date + timedelta(days=days) + relativedelta(years=years)
                # Calculate the number of days until the retention date
                days_until_retention = (new_date - date).days
                
                retention['Days'] = days_until_retention
                object_lock_config['Rule'] = {'DefaultRetention': retention}

            metadata = self.metadata_manager.get_bucket_metadata(bucket)
            if not metadata:
                raise KeyError(f"Bucket '{bucket}' not found in metadata")

            metadata["ObjectLock"] = object_lock_config
            
            # Save the updated metadata based on sync/asynchronous mode
            await self.metadata_manager.save_metadata(is_sync)

            return {"ObjectLock": object_lock_config}
        
        except ValueError as e:
            return {'Error': f'Invalid value: {str(e)}'}
        except KeyError as e:
            return {'Error': f'Metadata issue: {str(e)}'}
        except Exception as e:
            return {'Error': f'Unexpected error: {str(e)}'}
