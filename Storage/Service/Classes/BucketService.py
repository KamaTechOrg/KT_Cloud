from Abc import STOE
from Storage.Service.Classes.ObjectService import ObjectService
from ...DataAccess.ObjectManager import ObjectManager
from Validation import is_valid_bucket_name, is_valid_policy_name
from ...Models.BucketModel import Bucket
from ...Models.ObjectModel import ObjectModel
class BucketService(STOE):
    # כל פונקציה צריכה: לעדכן ב:1.מחלקות 2.objectManager3.storageManager
    def __init__(self):
        self.object_manager = ObjectManager()
        self.object_service = ObjectService()
        
    def create(self,*args, **kwargs):
        """Create a new bucket."""
        pass

    def delete(self, *args,**kwargs):
        """Delete an existing storage object."""
        pass

    def get(self, *args, **kwargs):
        """get storage object."""
        pass

    def put(self, *args, **kwargs):
        """put storage object."""
        pass

    def list(self, *args, **kwargs):
        """list storage object."""
        pass

    def head(self, *args, **kwargs):
        """check if object exists and is accessible with the appropriate user permissions."""
        pass

    def put_bucket_encryption(self, bucket:Bucket, is_encrypted = True):
        if bucket.name not in self.object_manager.get_buckets():
            raise FileNotFoundError("bucket or key not exist")
        bucket.encrypt_mode= True
        for obj in bucket.objects.values():
            obj:ObjectModel
            obj_content= self.object_manager.read(bucket.name, obj.key)
            encrypted_data, obj_encrypt_config = obj.encryption.encrypt_content(obj_content)
            self.object_service.put(obj, encrypted_data, obj_encrypt_config)

            