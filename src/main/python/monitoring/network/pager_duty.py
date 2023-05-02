import datetime
import json

import requests


class PagerDutyClient:
    """
    Attributes and methods to deal with PagerDuty activities
    """

    token: str = None
    routing_key: str = None
    hostname: str = None

    @classmethod
    def create_event_helper(cls, summary: str, source: str, severity: str, timestamp: str = None, component: str = None,
                            group: str = None, _class: str = None, custom_details: dict = None, images: dict = None,
                            links: dict = None, dedup_key=None) -> dict:
        """
        Create a PagerDuty event
        :param summary: Summary
        :param source: Source
        :param severity: Severity
        :param timestamp: Timestamp
        :param component: Component
        :param group: Group
        :param _class: Class
        :param custom_details: Custom Details
        :param images: Images
        :param links: Links
        :param dedup_key: Dedup key (if available)
        :return: Response status
        """
        map_dict = {
            "payload.timestamp": timestamp,
            "payload.component": component,
            "payload.group": group,
            "payload.class": _class,
            "payload.custom_details": custom_details,
            "images": images,
            "links": links,
            "dedup_key": dedup_key
        }

        headers_dict = {"Accept": "application/vnd.pagerduty+json;version=2", "Content-Type": "application/json"}

        payload_dict = {
            "payload": {
                "summary": summary,
                "source": source,
                "severity": severity
            },
            "event_action": "trigger",
            "routing_key": cls.routing_key
        }

        for key, value in map_dict.items():
            if value is not None:
                if key.startswith("payload."):
                    payload_dict["payload"][key.replace("payload.", "")] = value
                else:
                    payload_dict[key] = value

        response = requests.post(cls.hostname, data=json.dumps(payload_dict), headers=headers_dict)

        if response.status_code == 202:
            print(f'Alert created successfully: "{response.json()}"')

            return response.json()
        else:
            print(f"Error: '{response.text}'")

    @classmethod
    def create_incident(cls, app_dict: dict,
                        ts: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)) -> dict:
        """
        Create an incident in PagerDuty
        :param app_dict: Application dictionary
        :param ts: Created timestamp
        :return: Response along with created timestamp
        """
        ts_str = cls.get_date_str(ts)
        return {
            "response":
                PagerDutyClient.create_event_helper(
                    summary=f"{app_dict['type'].capitalize()} pipeline '{app_dict['name']}' not running as "f"of {ts}",
                    source=f"RWI-{app_dict['name']}", severity="critical", timestamp=ts_str,
                    custom_details=app_dict.get("customDetails")),
            "timestamp": ts
        }

    @classmethod
    def get_date_str(cls, dt=datetime.datetime.now(datetime.timezone.utc)) -> str:
        """
        Get date in specific format
        :param dt: Date
        :return: String date in the defined format
        """
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    @classmethod
    def get_incidents(cls):
        headers_dict = {
            "Accept": "application/vnd.pagerduty+json;version=2", "Content-Type": "application/json",
            "Authorization": f"Token token={cls.token}"
        }
        hostname = f"https://api.pagerduty.com/incidents"
        response = requests.get(hostname, headers=headers_dict)
        print(f"Response: '{response.json()}'")

    @classmethod
    def get_incident(cls, event_id):
        headers_dict = {
            "Accept": "application/vnd.pagerduty+json;version=2", "Content-Type": "application/json",
            "Authorization": f"Token token={cls.token}"
        }
        hostname = f"https://api.pagerduty.com/incidents/{event_id}"
        response = requests.get(hostname, headers=headers_dict)
        print(f"Response: '{response.json()}'")
