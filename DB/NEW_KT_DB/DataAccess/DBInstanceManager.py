import os
import sys
from typing import Dict, Any, List
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DataAccess.ObjectManager import ObjectManager
from Models.DBInstanceModel import DBInstance

class DBInstanceManager:
    def __init__(self, object_manager:ObjectManager):
        self.object_manager = object_manager
        self.object_manager.create_management_table(object_manager.convert_object_name_to_management_table_name( DBInstance.table_name), DBInstance.table_structure)

    def createInMemoryDBInstance(self,db_instance:DBInstance):
        self.object_manager.save_in_memory(self.object_manager.convert_object_name_to_management_table_name(DBInstance.table_name),db_instance)


    def deleteInMemoryDBInstance(self,db_instance_identifier:str):
        self.object_manager.delete_from_memory(criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'",object_name=self.object_manager.convert_object_name_to_management_table_name(DBInstance.table_name))


    def describeDBInstance(self,db_instance_identifier:str):
        return self.object_manager.get_from_memory(criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'",object_name=self.object_manager.convert_object_name_to_management_table_name(DBInstance.table_name))[db_instance_identifier]


    def modifyDBInstance(self,db_instance_identifier,updates):
        self.object_manager.update_in_memory(criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'",object_name=self.object_manager.convert_object_name_to_management_table_name(DBInstance.table_name),updates=updates)
    
    def is_db_instance_identifier_exist(self, db_instance_identifier: int) -> bool:
        '''Check if an db_instance with the given ID exists in the database.'''
        result= self.object_manager.db_manager.select_and_return_records_from_table(self.object_manager.convert_object_name_to_management_table_name(DBInstance.table_name),criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'")
        return bool(result)


# class DBSubnetGroupManager:
#     def __init__(self, object_manager):
#         self.object_manager = object_manager
#         self.object_manager._create_management_table(DBSubnetGroup.table_name, DBSubnetGroup.table_structure)

#     def create(self, subnet_group: DBSubnetGroup):
#         self.object_manager.save_in_memory(DBSubnetGroup.table_name, subnet_group)

#     def get(self, name: str):
#         data = self.object_manager.get_from_memory_by_id(DBSubnetGroup.pk_column, DBSubnetGroup.table_name, name)
#         if data:
#             data_mapping = {'db_subnet_group_name':name}
#             for key, value in data[name].items():
#                 data_mapping[key] = value 
#             return DBSubnetGroup(**data_mapping)
#         else:
#             raise ValueError(f"subnet group with name '{name}' not found")

    
#     def delete(self, name: str):
#         self.object_manager.delete_from_memory_by_id(DBSubnetGroup.pk_column, name, DBSubnetGroup.table_name)

#     def describe(self, name: str): 
#         return self.get(name).to_dict()
    
#     def modify(self, subnet_group: DBSubnetGroup):
#         updates = subnet_group.to_dict()
#         del updates['db_subnet_group_name']
#         self.object_manager.update_in_memory_by_id(DBSubnetGroup.pk_column, DBSubnetGroup.table_name, updates, subnet_group.db_subnet_group_name)
