from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager


def validate_policy_name(policy_manager: PolicyManager, policy_name: str):
    if not policy_manager.is_identifier_exist(policy_name):
        raise ValueError(f'Policy {policy_name} does not exist')
