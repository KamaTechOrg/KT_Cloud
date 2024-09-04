from Services import UserService
class UserController:
    def __init__(self, service: UserService):
        self.service = service

    def create_user(self, user_name, password, roles = [], policies = [], quotas = None):
        self.service.create(user_name, password, roles, policies, quotas)

    def delete_user(self, user_id):
        self.service.delete(user_id)

    def update_user_name(self, user_id, user_name):
        self.service.modify(user_id, user_name)

    def get_user_details(self, user_id):
        self.service.describe(user_id)

    def list_users(self):
        self.service.get_all_users()


    def assign_policy(self, user_id, permission):
        pass

    def revoke_policy(self, user_id, permission):
        pass

    def add_to_group(self, user_id, group_id):
        pass

    def remove_from_group(self, user_id, group_id):
        pass

    def set_quota(self, user_id, quota):
        pass

    def get_quote(self, user_id):
        pass

    def add_quota(self, quota):#user_id
        pass

    def check_quota(self, quota_name, amount):
        pass

    def can(self, action, resource):
        pass

    def verify_password(self, password):
        pass

