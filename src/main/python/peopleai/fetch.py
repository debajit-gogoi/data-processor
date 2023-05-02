import os
import sys
import time
import json
import logging
import requests
from datetime import datetime, timedelta
from requests.exceptions import ChunkedEncodingError

import pandas as pd
import configparser


class PeopleAIDataIncrementalPull:
    def __init__(self, configuration, logger, ingestion_datetime, start_date, end_date, job_id=None):
        self.people_ai_config_info = configuration
        self.my_logger = logger
        self.ingestion_datetime = ingestion_datetime
        self.start_date = start_date
        self.end_date = end_date
        self.job_id = job_id
        self.ingestion_status = True
        self.activity_type = 'All'
        self.access_token = self.get_access_token()

    def get_access_token(self):
        response = requests.post("https://api.people.ai/auth/v1/tokens",
                                 data={"client_id": self.people_ai_config_info['client_id'],
                                       "client_secret": self.people_ai_config_info['client_secret']})

        if response.status_code != 200:
            self.my_logger.info(
                '[FAILED]Connection establishment failed with client_id - {} and client_secret {} '.format(
                    self.people_ai_config_info['client_id'], self.people_ai_config_info['client_secret']))
            self.ingestion_status = False
            raise RuntimeError(response.text)

        token_data = response.json()
        access_token = token_data["access_token"]

        self.my_logger.info('Connection established succesfully and access_token - {} '.format(access_token))
        return access_token

    def create_and_trigger_job(self):

        job = self.post("/jobs", {"start_date": str(self.start_date.date()), "end_date": str(self.end_date.date()),
                                  "activity_type": self.activity_type,
                                  "export_type": "Delta",  # Delta or Snapshot
                                  "output_format": "JSONLines",  # JSONLines or JSON
                                  }, 'https://api.people.ai/pull/v1/export/activities')

        self.my_logger.info('Job started...Job - {}'.format(job['job_id']))
        return job

    def post(self, url, data, base_url):

        response = requests.post(url if url.startswith("https") else base_url + url, headers={
            "Authorization": "Bearer " + self.access_token,
            'Content-Type': 'application/json'
        }, data=json.dumps(data))

        if response.status_code != 200:
            self.my_logger.info('Request to get the data with url - {} and parameters - {} failed '.format(url, data))
            raise RuntimeError(response.text)

        data = response.json()
        return data

    def get_state(self, id, base_url):
        return self.get("/jobs/{}".format(id), base_url)["state"]

    def get(self, url, base_url):

        response = requests.get(url if url.startswith("https") else base_url + url, headers={
            "Authorization": "Bearer " + self.access_token,
        })

        if response.status_code != 200:
            raise RuntimeError(response.text)

        data = response.json()
        return data

    def wait_for_job_to_get_completed(self, job):
        job_id = job['job_id']
        state = self.get_state(job_id, self.people_ai_config_info['base_url'])
        self.my_logger.info('Waiting for Job {}...\n'.format(job_id))
        while state == "Running" or state == 'Queued':
            state = self.get_state(job_id, self.people_ai_config_info['base_url'])
            self.my_logger.info('DateTime - {} and State - {} '.format(str(datetime.now()), state))
            time.sleep(20)
        if self.get_state(job_id, self.people_ai_config_info['base_url']) == 'Completed':
            self.my_logger.info('[JOB COMPLETED] - Job has completed and will start pulling the data')
        return job_id

    def create_people_ai_job_to_pull_data_using_api(self):

        if self.job_id is None:
            job = self.create_and_trigger_job()
            self.job_id = self.wait_for_job_to_get_completed(job)

        self.my_logger.info('Job {} completed and fetching the data ...\n'.format(self.job_id))

        job_summary = self.get("/jobs/{}".format(self.job_id), self.people_ai_config_info['base_url'])

        self.my_logger.info('Job summary - {} '.format(job_summary))
        fragments = job_summary['fragments']
        self.my_logger.info('Fragements - {} '.format(fragments))

        with requests.get(self.people_ai_config_info['base_url'] + "/jobs/{}/data".format(self.job_id),
                          headers={"Authorization": "Bearer " + self.access_token}, stream=False) as response:
            if response.encoding is None:
                response.encoding = 'utf-8'
            self.decode_people_ai_api_json_response(response)

    def decode_people_ai_api_json_response(self, api_json_responses):

        all_parsed_responses = []
        campaign_parsed_responses = []
        participant_parsed_responses = []
        activity_stats_parsed_responses = []
        crm_status_parsed_responses = []

        data_dict = {
            "all": all_parsed_responses,
            "campaign": campaign_parsed_responses,
            "participant": participant_parsed_responses,
            "activity": activity_stats_parsed_responses,
            "crm": crm_status_parsed_responses
        }

        generic_attributes_json_keys = ['source_id', 'type', 'created', 'subject', 'summary', 'direction', 'timestamp',
                                        'ingest_timestamp', 'tags', 'in_reply_to_source_id',
                                        'follow_up_email_source_id', 'reply_received', 'attachment_file_names',
                                        'start_time', 'end_time', 'duration', 'all_day', 'location',
                                        'followed_meeting_source_id', 'outbound_phone_number', 'status']
        campaign_attributes_json_keys = ['crm_id', 'name', 'start_date', 'end_date']
        participants_attributes_json_keys = ['peopleai_id', 'crm_id', 'name', 'title', 'phone', 'email', 'role', 'type',
                                             'time_spent', 'team', 'matched_as', 'status']
        activity_stats_json_keys = ['total_participants', 'internal_participants', 'external_participants',
                                    'contacts_participants', 'participants_on_to_line', 'participants_on_cc_line',
                                    'accepted_participants', 'declined_participants', 'tentative_participants']

        crm_status_json_keys = ['pushed', 'push_time', 'crm_id', 'matched_to']
        crm_matched_json_keys = ['crm_id', 'name', 'email', 'parent_account_crm_id', 'parent_account_name',
                                 'stage_name', 'stage_rank']

        try:

            record_counter = 0
            invalid_record_counter = 0

            self.my_logger.info('[STARTED-PROCESSING] Started Processing file contents')


            self.write_file([json.loads(record) for record in api_json_responses.iter_lines(decode_unicode=True)],
                            file_name="api-response")

            for record in api_json_responses.iter_lines(decode_unicode=True):

                response_record = ""
                try:
                    response_record = json.loads(record)
                except Exception as exception:
                    self.my_logger.info(
                        '[EXCEPTION] Exception has occured while parsing the record - {}'.format(exception))
                    continue

                record_counter += 1

                if 'uid' not in response_record:
                    self.my_logger.info('[UID_NOT_PRESENT-IGNORE] UID is not available.. Ignoring the record')
                    continue

                uid = response_record['uid']
                self.extract_people_ai_response_generic_attributes(generic_attributes_json_keys, response_record,
                                                                   all_parsed_responses, uid, True)

                self.extract_people_ai_response_generic_attributes(activity_stats_json_keys, response_record['stats'],
                                                                   activity_stats_parsed_responses, uid, False,
                                                                   response_record['type'])

                if 'campaigns' in response_record:
                    for campaign in response_record['campaigns']:
                        self.extract_people_ai_response_generic_attributes(campaign_attributes_json_keys, campaign,
                                                                           campaign_parsed_responses, uid)

                if 'participants' in response_record:
                    for participant in response_record['participants']:
                        self.extract_people_ai_response_generic_attributes(participants_attributes_json_keys,
                                                                           participant,
                                                                           participant_parsed_responses, uid)

                self.extract_crm_status_and_associate_matches(crm_status_json_keys, crm_matched_json_keys,
                                                              response_record['crm_status'],
                                                              crm_status_parsed_responses,
                                                              uid)
                # Perform bulk update instead of updating record by record
                if (record_counter % 1000 == 0):
                    self.my_logger.info(
                        'Performing bulk ingestion with size limit and record count - {} '.format(record_counter))
                    """
                    self.perform_bulk_ingestion_onto_postgresql(all_parsed_responses, campign_parsed_responses,
                                                           partificant_parsed_responses,
                                                           activity_stats_parsed_responses, crm_status_parsed_responses)

                    all_parsed_responses = []
                    campaign_parsed_responses = []
                    participant_parsed_responses = []
                    activity_stats_parsed_responses = []
                    crm_status_parsed_responses = []
                    """

            self.write_data_dict(data_dict)
            self.my_logger.info(
                '[Final Update ]Performing bulk ingestion and Total records processed - {} '.format(record_counter))
            """
            self.perform_bulk_ingestion_onto_postgresql(all_parsed_responses, campign_parsed_responses,
                                                   partificant_parsed_responses, activity_stats_parsed_responses,
                                                   crm_status_parsed_responses)
            """

            self.my_logger.info(
                '[ENDING-PROCESSING] Ending Processing file contents and total invalid_records - {}'.format(
                    invalid_record_counter))

        except ChunkedEncodingError as chunnedException:
            self.my_logger.info(
                '[Exception - {}] Connection closed exception has occured and we should continue'.format(
                    chunnedException))
            raise
        except Exception as exception:

            # self.my_logger.info("Record - {} ".format(str(record)))
            self.ingestion_status = False
            self.my_logger.info(
                '[Exception - {}] Exception has occured while processing/ingesting the data onto PG'.format(exception))
            raise

    def extract_people_ai_response_generic_attributes(self, json_keys, api_response, parsed_responses, uid,
                                                      is_load_date_required=False, request_type=None):

        parsed_response = []
        parsed_response.append(uid)
        if (request_type != None):
            parsed_response.append(request_type)

        for json_key in json_keys:
            if (json_key == 'tags' or json_key == 'attachment_file_names'):
                if (json_key in api_response and len(api_response[json_key]) != 0):
                    parsed_response.append(str(','.join([tag for tag in api_response[json_key]])))
                else:
                    parsed_response.append(None)
            elif (json_key == 'team' and 'team' in api_response):
                if 'name' in api_response[json_key]:
                    parsed_response.append(api_response[json_key]['name'])
                else:
                    parsed_response.append(None)
            elif json_key in api_response:
                parsed_response.append(api_response[json_key])
            else:
                parsed_response.append(None)

        if (is_load_date_required):
            parsed_response.append(self.ingestion_datetime)

        parsed_responses.append(tuple(parsed_response))

    def extract_crm_status_and_associate_matches(self, crm_status_json_keys, crm_matched_json_keys, crm_status_record,
                                                 crm_status_parsed_responses, uid):

        crm_status_parsed_response = []

        crm_status_parsed_response.append(uid)
        for json_key in crm_status_json_keys:
            if json_key in crm_status_record:
                crm_status_parsed_response.append(crm_status_record[json_key])
            else:
                crm_status_parsed_response.append(None)

        if 'account' in crm_status_record:
            crm_account_record = crm_status_record['account']
            for acc_field in ['crm_id', 'name']:
                if acc_field in crm_account_record:
                    crm_status_parsed_response.append(crm_account_record[acc_field])
                else:
                    crm_status_parsed_response.append(None)
        else:
            crm_status_parsed_response.append(None)
            crm_status_parsed_response.append(None)

        if 'matched_to' in crm_status_record and crm_status_record['matched_to'] != 'none':
            matched_record = crm_status_record['matched_to']
            for json_key in crm_matched_json_keys:
                if matched_record in crm_status_record and json_key in crm_status_record[matched_record]:
                    crm_status_parsed_response.append(crm_status_record[matched_record][json_key])
                else:
                    crm_status_parsed_response.append(None)
        else:
            for json_key in crm_matched_json_keys:
                crm_status_parsed_response.append(None)

        crm_status_parsed_responses.append(tuple(crm_status_parsed_response))

    def write_data_dict(self, data_dict: dict):

        column_dict = {
            "all": ['uid', 'source_id', 'request_type', 'created_date', 'subject', 'summary', 'direction',
                    'event_timestamp', 'ingest_timestamp', 'tags', 'in_reply_to_source_id',
                    'follow_up_email_source_id', 'reply_received', 'attachment_file_names', 'start_time',
                    'end_time', 'duration', 'all_day', 'location', 'followed_meeting_source_id',
                    'outbound_phone_number', 'status', 'load_date_time'],
            "campaign": ['uid', 'crm_id', 'crm_name', 'start_date', 'end_date'],
            "participant": ['uid', 'peopleai_id', 'crm_id', 'name', 'title', 'phone', 'email', 'role', 'type',
                            'time_spent', 'team', 'matched_as', 'status'],
            "activity": ['uid', 'request_type', 'total_participants', 'internal_participants', 'external_participants',
                         'contacts_participants', 'participants_on_to_line', 'participants_on_cc_line',
                         'accepted_participants', 'declined_participants', 'tentative_participants'],
            "crm": ['uid', 'pushed', 'push_time', 'task_id', 'matched_to', 'acc_crm_id', 'acc_crm_name', 'crm_id',
                    'name', 'email', 'parent_account_crm_id', 'parent_account_name', 'stage_name', 'stage_rank']
        }

        for file_name, data in data_dict.items():

            if file_name in column_dict:
                df = pd.DataFrame(data, columns=column_dict[file_name])
                if file_name == "all":
                    df[column_dict["all"][-1]] = df[column_dict["all"][-1]].dt.strftime("%Y-%m-%d %H:%M:%S.%s")
            else:
                df = pd.DataFrame(data)

            self.write_file(df.to_dict("records"), file_name)

    def write_file(self, data, file_name):

        file_path = (f"data/{self.job_id}-{file_name}-{self.start_date.strftime('%Y%m%dT%H%M%S')}"
                     f"-{self.end_date.strftime('%Y%m%dT%H%M%S')}.json")
        self.my_logger.info(f"Writing file '{file_path}'")

        with open(file_path, mode="w", encoding="utf-8") as fp:
            fp.write(json.dumps(data, separators=(",", ":")))

    @classmethod
    def get_config(cls, path):
        cp = configparser.ConfigParser()
        cp.read(path)
        return cp

    @classmethod
    def setup_logging(cls, log_path: str) -> logging.Logger:
        if not os.path.exists(os.path.dirname(log_path)):
            os.mkdir(os.path.dirname(log_path))

        logger = logging.getLogger(__name__)

        # Create handlers
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(log_path)
        c_handler.setLevel(logging.DEBUG)
        f_handler.setLevel(logging.DEBUG)

        # Create formatters and add it to handlers
        format_str = "%(asctime)s - %(levelname)s - %(message)s"
        c_format = logging.Formatter(format_str)
        f_format = logging.Formatter(format_str)
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)

        logger.setLevel(logging.DEBUG)

        return logger


def main():
    config = PeopleAIDataIncrementalPull.get_config("config.ini")["people.ai"]
    logger = PeopleAIDataIncrementalPull.setup_logging("log/people.ai.log")
    ingestion_datetime = datetime.now()
    start_date = datetime.strptime("2022-07-13T00:00:00", "%Y-%m-%dT%H:%M:%S")
    end_date = datetime.strptime("2022-07-13T23:59:59", "%Y-%m-%dT%H:%M:%S")
    pai = PeopleAIDataIncrementalPull(config, logger, ingestion_datetime, start_date, end_date)
    pai.create_people_ai_job_to_pull_data_using_api()


if __name__ == "__main__":
    main()

"""
{"campaigns":[],"crm_status":{"matched_to":"lead","pushed":false,"lead":{"crm_id":"00Q0e00001XpKDpEAN","name":"Cedric Deffo Sikounmo","title":"Dir / Resp R&D","phone":"+33 4 72 14 68 90","email":"cedric.deffo-sikounmo@hdtechnology.fr"}},"direction":"outbound","duration":60,"ingest_timestamp":"2022-07-12T07:13:58","outbound_phone_number":"+33472146890","participants":[{"role":"originator","type":"internal","time_spent":60,"team":{"crm_id":"520173564","name":"EMEA France & North West Africa"},"peopleai_id":"1915448717","crm_id":"0057V0000099i1HQAQ","name":"Sarah Milles","title":"Associate Sales Development Representative","email":"sarah.milles@debajit.com","phone":"+33668532002"},{"role":"participant","type":"external","time_spent":60,"matched_as":"lead","peopleai_id":"1247050227","crm_id":"00Q0e00001XpKDpEAN","name":"Cedric Deffo Sikounmo","title":"Head R&D et Innovation","email":"cedric.deffo-sikounmo@hdtechnology.fr"}],"source_id":"00T7V00008tiFjzUAE","stats":{"total_participants":2,"internal_participants":1,"external_participants":1,"contacts_participants":0},"tags":["call","logged_call"],"timestamp":"2022-07-11T00:00:00","uid":"00T7V00008tiFjzUAE","type":"call"}
"""
