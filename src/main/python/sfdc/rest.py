import requests
import urllib.parse

from simple_salesforce import Salesforce


class RESTClient:
    @classmethod
    def authenticate(cls):
        pass

    @classmethod
    def get_session_id(cls):
        pass

    @staticmethod
    def get_url_encoded_data(data_dict: dict) -> str:
        return "&".join([f"{key}={urllib.parse.quote_plus(value)}" for key, value in data_dict.items()])

    @classmethod
    def get_session_id(cls, sf: Salesforce) -> str:
        session_id = None
        for element in dir(sf):
            if element == 'session_id':
                session_id = f'{getattr(sf, element)}'
        return session_id
