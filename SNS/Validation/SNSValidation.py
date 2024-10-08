from SNS.DataAccess.SNSManager import SNSTopicManager
from SNS.Model.SNSModel import Protocol
import re


def validate_topic_name(sns_manager: SNSTopicManager, topic_name: str):
    '''Validate topic name.'''
    if not topic_name:
        raise ValueError('Topic name cannot be empty.')
    if sns_manager.is_topic_exist(topic_name):
        raise ValueError('Topic already exists.')


def validate_topic_name_exist(sns_manager: SNSTopicManager, topic_name: str):
    '''Validate topic name does not exist.'''
    if not topic_name:
        raise ValueError('Topic name cannot be empty.')
    if not sns_manager.is_topic_exist(topic_name):
        raise ValueError('Topic does not exist.')


def validate_protocol(protocol: Protocol):
    '''Validate protocol.'''
    if protocol != Protocol.EMAIL.value:
        raise ValueError('For now, KT-SNS supports only email protocol.')


def validate_endpoint(protocol: Protocol, endpoint: str):
    '''Validate endpoint.'''
    protocol_validations[protocol](endpoint)


def validate_endpoint_exist(sns_manager: SNSTopicManager, topic_name: str, protocol: Protocol, endpoint: str):
    '''Validate endpoint exists.'''
    subscribers = sns_manager.get_topic(topic_name).subscribers
    if protocol not in subscribers or endpoint not in subscribers[protocol]:
        raise ValueError('Endpoint does not exist.')


def validate_email_address(email_address: str):
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', email_address):
        raise ValueError('Invalid email address.')


protocol_validations = {
    Protocol.EMAIL.value: validate_email_address
}
