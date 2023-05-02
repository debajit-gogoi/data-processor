import logging
import requests
import urllib.parse


class YARNClient:
    """
    Attributes and methods to deal with YARN REST API
    """
    hostname: str = None
    port_resource_manager: int = None
    port_history_server: int = None
    timeout = 30
    logger = logging.getLogger()

    @classmethod
    def get_url_resource_manager(cls) -> str:
        """
        Get Resource Manager URL <host:port>
        :return: URL of Resource Manager
        """
        return f"{cls.hostname}:{cls.port_resource_manager}"

    @classmethod
    def get_url_history_server(cls) -> str:
        """
        Get Job History Server URL <host:port>
        :return: URL of Job History Server
        """
        return f"{cls.hostname}:{cls.port_history_server}"

    @classmethod
    def get_rm_json(cls, resource: str) -> dict:
        """
        Get JSON (converted to dictionary) resource on the Resource Manager
        :param resource: End point of the resource
        :return: Dictionary of the resource JSON
        """
        try:
            try:
                return requests.get(urllib.parse.urljoin(cls.get_url_resource_manager(), resource), verify=False,
                                    timeout=cls.timeout).json()
            except requests.exceptions.SSLError as ssle:
                cls.logger.debug(f"requests.exceptions.SSLError: '{ssle}'")
        except requests.exceptions.ConnectionError as ce:
            cls.logger.error(f"requests.exceptions.ConnectionError: '{ce}'")

    @classmethod
    def get_jh_html(cls, resource: str) -> str:
        """
        Get HTML (string) resource on Job History Server
        :param resource: End point of the resource
        :return: String (HTML) of the resource (log)
        """
        try:
            try:
                return requests.get(urllib.parse.urljoin(cls.get_url_history_server(), resource), verify=False,
                                    timeout=cls.timeout).text
            except requests.exceptions.SSLError as ssle:
                cls.logger.debug(f"requests.exceptions.SSLError: '{ssle}'")
        except requests.exceptions.ConnectionError as ce:
            cls.logger.error(f"requests.exceptions.ConnectionError: '{ce}'")

    @classmethod
    def get_apps(cls):
        """
        Get the list of past and running apps
        :return: Dictionary of apps
        """
        return cls.get_rm_json("ws/v1/cluster/apps")

    @classmethod
    def get_attempts(cls, app_id: str) -> dict:
        """
        Get all the attempts of a YARN application
        :param app_id: YARN application ID
        :return: Get dictionary of application attempts
        """
        return cls.get_rm_json(f"ws/v1/cluster/apps/{app_id}/appattempts")

    @classmethod
    def get_containers(cls, app_id: str, attempt_id: str) -> dict:
        """
        Get all the container details of an attempt ID (of a YARN application)
        :param app_id: YARN application ID
        :param attempt_id: Attempt ID
        :return: Get dictionary of containers
        """
        return cls.get_rm_json(f"ws/v1/cluster/apps/{app_id}/appattempts/{attempt_id}/containers")

    @classmethod
    def get_container_logs(cls, container_id: str, log_type: str = "stderr") -> str:
        """
        Get HTML (string) of logs of a container
        :param container_id: Container ID of an attempt ID (of a YARN application)
        :param log_type: Type of log <stdout/ stderr>
        :return: String of HTML page
        """
        return cls.get_jh_html(f"node/containerlogs/{container_id}/hdfs/{log_type}/?start=0")
