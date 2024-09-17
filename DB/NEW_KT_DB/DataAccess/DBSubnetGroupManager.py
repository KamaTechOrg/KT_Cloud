from typing import Dict, Any, List

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DataAccess import ObjectManager
from Models.DBSubnetGroupModel import DBSubnetGroup

class DBSubnetGroupManager:
    def __init__(self, object_manager: ObjectManager):
        self.object_manager = object_manager
        self.object_manager.create_management_table(DBSubnetGroup.table_name, DBSubnetGroup.table_structure)

    def create(self, subnet_group: DBSubnetGroup):
        self.object_manager.insert_object_to_management_table(DBSubnetGroup.table_name, subnet_group.to_sql())

    def get(self, name: str):
        data = self.object_manager.get_from_memory(pk_col = DBSubnetGroup.pk_column, table_name = DBSubnetGroup.table_name, object_id = name)
        if data:
            data_mapping = {'db_subnet_group_name':name}
            for key, value in data[name].items():
                data_mapping[key] = value 
            return DBSubnetGroup(**data_mapping)
        else:
            raise ValueError(f"subnet group with name '{name}' not found")

    
    def delete(self, name: str):
        self.object_manager.delete_from_memory_by_id(DBSubnetGroup.pk_column, name, DBSubnetGroup.table_name)

    def describe(self, name: str): 
        return self.get(name).to_dict()
    
    def modify(self, subnet_group: DBSubnetGroup):
        updates = subnet_group.to_dict()
        del updates['db_subnet_group_name']
        self.object_manager.update_in_memory_by_id(DBSubnetGroup.pk_column, DBSubnetGroup.table_name, updates, subnet_group.db_subnet_group_name)
