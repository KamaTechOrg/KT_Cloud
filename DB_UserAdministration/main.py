import os
import sys


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DB_UserAdministration.Controllers.PolicyController import PolicyController
from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager
from DB_UserAdministration.Services.PolicyService import PolicyService


if __name__ == "__main__":
    manager = PolicyManager("./DB_UserAdministration/DataAccess/data_access_architecture_test.db")
    service = PolicyService(manager)
    controller = PolicyController(service)
    
    #controller.create_policy('policy_1')
    # print(controller.list_policies())
    # print(controller.get_policy('object_2'))
    # controller.delete_policy('object_2')
    print(manager.describe())