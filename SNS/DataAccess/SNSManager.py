from collections import defaultdict
import json
from typing import List
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from SNS.Model.SNSModel import Protocol, SNSTopicModel


class SNSTopicManager:

    def __init__(self, object_manager: ObjectManager):
        self.object_manager = object_manager
        self.object_manager.create_management_table(
            SNSTopicModel.get_object_name(), SNSTopicModel.table_schema, 'TEXT')

    def create_topic(self, sns_model: SNSTopicModel):
        self.object_manager.save_in_memory(
            SNSTopicModel.get_object_name(), sns_model.to_sql())

    def delete_topic(self, topic_name: str) -> None:
        self.object_manager.delete_from_memory_by_pk(
            SNSTopicModel.get_object_name(), SNSTopicModel.pk_column, topic_name)

    def get_topic(self, topic_name: str) -> SNSTopicModel:
        sns_topic_list = self.object_manager.get_from_memory(
            SNSTopicModel.get_object_name(), '*', f'{SNSTopicModel.pk_column} =  "{topic_name}"')
        if not sns_topic_list:
            raise ValueError(f"Topic {topic_name} not found")
        topic_name, subscribers = sns_topic_list[0]
        return SNSTopicModel(topic_name, json.loads(subscribers))

    def update_topic(self, sns_model: SNSTopicModel):
        sns_dict = sns_model.to_dict()
        updates = ', '.join([f"{key}='{json.dumps(value) if isinstance(value, dict) else value}'" for key, value in sns_dict.items()])
        self.object_manager.update_in_memory(
            SNSTopicModel.get_object_name(), updates, f'{sns_model.pk_column} = "{sns_model.pk_value}"')

    def is_exist_topic(self, topic_name: str):
        try:
            self.get_topic(topic_name)
            return True
        except:
            return False
