import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Service.Classes import BucketPolicyService

class BucketPolicyController:
    def __init__(self, service: BucketPolicyService):
        self.service = service


    def create_bucket_policy(self, bucket_name = None, permissions = [], allow_versions = True):
        return self.service.create(bucket_name, permissions, allow_versions)


    def delete_bucket_policy(self, bucket_name):
        self.service.delete(bucket_name)


    # def put_bucket_object(self, updates):
    #     self.service.put(updates)
    

    def modify_bucket_policy(self, bucket_name, new_permmision=[],allow_versions=None):
        self.service.modify(bucket_name, new_permmision, allow_versions)


    def get_bucket_policy(self, bucket_name):
        return self.service.get(bucket_name)
        
