#!/bin/env python3
# -*- coding: utf-8 -*-
import os
import datetime
import re

from lxml import etree

import numpy as np
import pandas as pd
from sqlalchemy import exc

import db.models as models
from db.models import Monitoring
from util.pipeline import Pipeline
from network.yarn import YARNClient
from network.pager_duty import PagerDutyClient

from sqlalchemy import desc


# TODO: Check final status (Accepted/ Succeeded)

def main():
    """
    Entry point
    :return:
    """

    schedule_dict = read_config_files()

    pl_realtime_df = pd.DataFrame(schedule_dict["realtime"])
    pl_batch_df = pd.DataFrame(schedule_dict["batch"])

    logger.info(f"Fetching YARN applications")
    apps_dict = YARNClient.get_apps()

    if apps_dict is not None:
        app_df = pd.DataFrame(apps_dict["apps"]["app"])

        realtime_status(pl_realtime_df, app_df)
        batch_status(pl_batch_df, app_df)
    else:
        logger.error(f"Could not fetch YARN applications")


def realtime_status(schedule_df: pd.DataFrame, app_df: pd.DataFrame):
    """
    Check the status of Realtime application
    :param schedule_df: Schedule DF
    :param app_df: Realtime application DF pulled from YARN
    :return:
    """

    logger.debug(f"Realtime Pipeline DF:\n{schedule_df[['name']].to_dict('records')}\n")

    status_df = filter_pipeline(schedule_df, app_df)

    status_df["launchTime"] = Pipeline.epoch_to_local_ts(status_df, "launchTime")
    status_df["startedTime"] = Pipeline.epoch_to_local_ts(status_df, "startedTime")
    status_df["finishedTime"] = Pipeline.epoch_to_local_ts(status_df, "finishedTime")
    status_df["type"] = "realtime"

    status_df = (status_df
                 .sort_values(["name", "startedTime"], ascending=False)
                 .drop_duplicates("name", keep="first")
                 .sort_index())
    non_running_df = status_df[
        ~((status_df["state"] == "RUNNING") & (status_df["finalStatus"] == "UNDEFINED"))
    ].copy()

    logger.debug(f"Realtime DF:\n{status_df[['id', 'name', 'startedTime', 'state']].head().to_dict('records')}\n")
    logger.debug(f"Non-Running DF:\n{non_running_df[['name', 'state', 'startedTime', 'id']].head().to_dict('records')}\n")

    if not non_running_df.empty:
        non_running_df = nullify_nan_nat(non_running_df)
        logger.debug(f"Non-Running DF:\n{non_running_df.to_dict('records')}\n")
        for app_dict in non_running_df.to_dict("records"):
            logger.info(f"Realtime pipeline '{app_dict['name']}' is not running; creating an incident")
            raise_incident(app_dict, pipeline_type="realtime")
    else:
        logger.info("All realtime pipelines are running properly")

    # Check logs for errors
    check_container_logs_helper(status_df, pipeline_type="realtime")


def batch_status(schedule_df: pd.DataFrame, app_df: pd.DataFrame):
    """
    Check the status of Batch application
    :param schedule_df: Schedule DF
    :param app_df: Batch application DF pulled from YARN
    :return:
    """

    status_df = filter_pipeline(schedule_df, app_df)

    status_df["launchTime"] = Pipeline.epoch_to_local_ts(status_df, "launchTime")
    status_df["startedTime"] = Pipeline.epoch_to_local_ts(status_df, "startedTime")
    status_df["finishedTime"] = Pipeline.epoch_to_local_ts(status_df, "finishedTime")
    status_df["type"] = "batch"

    schedule_df = schedule_df.explode("scheduledAt")
    logger.debug(f"Batch Pipeline DF:\n{schedule_df.to_dict('records')}\n")

    ts = pd.to_datetime(datetime.datetime.now())
    ts_str = ts.date().strftime("%Y-%m-%d")

    schedule_df["startTime"] = schedule_df.apply(
        lambda row: datetime.datetime.strptime(f"{ts_str}T{row['scheduledAt']}", "%Y-%m-%dT%H:%M"), axis=1)
    schedule_df["endTime"] = schedule_df["startTime"] + pd.Timedelta(hours=1)

    schedule_df = schedule_df[((schedule_df["startTime"] >= ts) & (schedule_df["startTime"] <= schedule_df["endTime"]))]
    schedule_list = schedule_df.to_dict("records")
    logger.debug(f"Batch Schedule DF:\n{schedule_df.to_dict('records')}\n")

    latest_df = (status_df
                 .sort_values(["name", "finishedTime", "startedTime"], ascending=False)
                 .drop_duplicates("name", keep="first")
                 .sort_index())
    logger.debug(f"Batch DF:\n{latest_df[['id', 'name', 'startedTime', 'finishedTime']].to_dict('records')}\n")

    # TODO: Replace loop with one DF to another DF comparison
    for schedule_dict in schedule_list:
        miss_df = status_df[(
                (status_df["name"] == schedule_dict["name"]) &
                (status_df["startedTime"] >= schedule_dict["startTime"]) &
                (status_df["finishedTime"] <= schedule_dict["endTime"])
        )]

        if not miss_df.empty:
            miss_df = nullify_nan_nat(miss_df)
            missed_list = miss_df.to_dict("records")

            for missed_dict in missed_list:
                logger.info(
                    f"{schedule_dict['name']} did not run between {schedule_dict['startTime']} "
                    f"and {schedule_dict['endTime']}"
                )
                raise_incident(missed_dict, pipeline_type="batch")
        else:
            pass
            # logger.info(f"Batch pipeline '{schedule_dict['name']} ('{schedule_dict['startTime']}' to "
            #       f"'{schedule_dict['endTime']}') ran successfully")

    if not schedule_list:
        logger.info(f"All batch pipelines ran successfully")

    # Check logs for errors
    check_container_logs_helper(status_df, pipeline_type="batch")


def raise_incident(app_dict: dict, pipeline_type: str = "realtime"):
    """
    Create an incident in PagerDuty
    - If already not present or 30 min have passed and the pipeline has still been not up and running
    - Does not raise incident if already raised
    :param app_dict: Application dictionary
    :param pipeline_type: Type of the pipeline
    :return:
    """
    monitoring = (session.query(Monitoring)
                  .filter((Monitoring.appName == app_dict["name"]))
                  .order_by(Monitoring.appName, desc(Monitoring.pdCreatedTime)).first())

    # An incident has already been created
    if monitoring is not None and pipeline_type in ("realtime", "batch"):
        logger.info(f"Application '{app_dict['name']}' with ID '{app_dict['id']}' already exists with dedupKey "
                    f"'{monitoring.pdDedupKey}'")

        diff_td = datetime.datetime.now() - monitoring.pdCreatedTime

        # The incident has not been resolved for more than 30 min, hence raise another one
        if diff_td.seconds > 3600:
            logger.info(f"Event was created for more than 30 min ago")
            raise_incident_helper(app_dict)
        else:
            logger.info(f"Incident already exists and no need to raise again")

    # There's no entry of the App in the DB
    else:
        raise_incident_helper(app_dict)


def raise_incident_helper(app_dict: dict) -> bool:
    """
    Helper method to facilitate creation of an incident
    :param app_dict: Application dictionary
    :return: Status of the incident creation
    """
    status = False
    try:
        response_dict = PagerDutyClient.create_incident(app_dict)
        app_dict["dedup_key"] = response_dict["response"].get("dedup_key")
        app_dict["timestamp"] = response_dict["timestamp"]

        monitoring = Monitoring.from_dict(app_dict)
        logger.info(
            f"Adding application '{monitoring.appName}' with ID '{monitoring.appId}' and creating event with dedupKey"
            f" '{monitoring.pdDedupKey}'")
        session.add(monitoring)
        session.commit()
        status = True

    except exc.IntegrityError as ie:
        logger.info(f"IntegrityError: '{ie}'")
        session.rollback()
    except Exception as e:
        logger.info(f"Exception: '{e}'")
        session.rollback()

    return status


def filter_pipeline(schedule_df: pd.DataFrame, app_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given the schedule filter out the required applications pulled from the YARN REST API
    :param schedule_df: Schedule of realtime/ batch pipeline DF
    :param app_df: Application DF
    :return:
    """
    status_df = pd.merge(schedule_df, app_df, how="outer", left_on=["name"], right_on=["name"], indicator=True)
    status_df = status_df[((status_df["_merge"] == "both") | (status_df["_merge"] == "left_only"))].copy()

    if "errorLogPatterns" not in status_df.columns:
        status_df["errorLogPatterns"] = None

    return status_df[column_list]


def check_container_logs_helper(status_df: pd.DataFrame, pipeline_type: str) -> bool:
    """
    Helper function to check logs of all the containers of the pipelines
    :param status_df: Pipeline status DF
    :param pipeline_type: Type of the pipeline <realtime/ batch>
    :return: True if any error found in any of the containers of the pipeline else False
    """

    check_log_df = status_df.loc[(~status_df["errorLogPatterns"].isna() & status_df["errorLogPatterns"])].copy()

    status_list = []
    if not check_log_df.empty:
        check_log_df = nullify_nan_nat(check_log_df)
        logger.debug(f"Pipelines to check logs for:\n{check_log_df[['id', 'name']].to_dict('records')}\n")
        for app_dict in check_log_df.to_dict("records"):

            if check_container_logs(app_dict):
                logger.info(
                    f"{pipeline_type.capitalize()} pipeline '{app_dict['name']}' contains error; creating an incident")
                raise_incident(app_dict, pipeline_type=pipeline_type)
                status_list.append(True)

        if any(status_list):
            logger.info(f"Error(s) found in the logs of the containers of the {pipeline_type} pipelines")
        else:
            logger.info(f"No errors found in the logs of the containers of the {pipeline_type} pipelines")

    else:
        logger.debug(f"There are no {pipeline_type} pipelines to check logs for")

    return any(status_list)


def check_container_logs(app_dict: dict) -> bool:
    """
    Check logs of matching list of error patterns in the containers of a pipeline by:
    - Get the max of attempt IDs of the application
    - Check logs of each of the containers of the attempt matching error patterns
    :param app_dict: Dictionary with details of an application
    :return: True if any error found in any of the containers else False
    """

    app_id = app_dict["id"]
    error_pattern_list = app_dict["errorLogPatterns"]
    logger.debug(f"App ID: '{app_id}'")

    attempt_dict = YARNClient.get_attempts(app_id)

    log_status_list = []
    # TODO: Avoid deep nesting
    if attempt_dict is not None:
        if "appAttempts" in attempt_dict:
            if "appAttempt" in attempt_dict["appAttempts"]:
                attempt_df = pd.DataFrame(attempt_dict["appAttempts"]["appAttempt"])
                attempt_id = attempt_df["appAttemptId"].max()
                logger.debug(f"Attempt ID: '{attempt_id}'")
                container_list_dict = YARNClient.get_containers(app_id, attempt_id)

                for container_dict in container_list_dict["container"]:
                    container_id = container_dict["containerId"]
                    logger.info(f"Collecting 'stderr' logs of the container '{container_id}'")
                    container_log_str = YARNClient.get_container_logs(container_id)
                    match_result = any(
                        [pattern_in_html_log(container_log_str, error_pattern) for error_pattern in error_pattern_list]
                    )

                    if match_result:
                        logger.debug(f"Breaking out of loop since error found in the container logs")
                        log_status_list.append(match_result)
                        break
            else:
                logger.debug(f"No attempts found")
        else:
            logger.debug(f"No attempts found")

    return any(log_status_list)


def pattern_in_html_log(log_str: str, pattern: str) -> bool:
    """
    Parse HTML, extract log text and match error pattern
    :param log_str: HTML page in text
    :param pattern: RegEx pattern to search logs for
    :return: True if pattern found else false
    """
    tree = etree.HTML(log_str)
    xp_data = '//*[@id="layout"]/tbody/tr/td[2]/pre'

    if xp_data is not None:
        return True if re.findall(pattern, tree.xpath(xp_data)[0].text) else False

    return False


def nullify_nan_nat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace 'NAN' and 'NaT' from data to persist on DB
    :param df: Application DF
    :return: DF with replaced values
    """
    dtype_dict = {
        "id": "object",
        "user": "object",
        "name": "object",
        "queue": "object",
        "state": "object",
        "finalStatus": "object",
        "progress": "float64",
        "trackingUI": "object",
        "trackingUrl": "object",
        "diagnostics": "object",
        "clusterId": "int64",
        "applicationType": "object",
        "applicationTags": "object",
        "priority": "int64",
        "startedTime": "datetime64[ns]",
        "launchTime": "datetime64[ns]",
        "finishedTime": "datetime64[ns]",
        "elapsedTime": "float64",
        "amContainerLogs": "object",
        "amHostHttpAddress": "object",
        "allocatedMB": "int64",
        "allocatedVCores": "int64",
        "reservedMB": "int64",
        "reservedVCores": "int64",
        "runningContainers": "int64",
        "memorySeconds": "int64",
        "vcoreSeconds": "int64",
        "queueUsagePercentage": "float64",
        "clusterUsagePercentage": "float64",
        "resourceSecondsMap": "object",
        "preemptedResourceMB": "int64",
        "preemptedResourceVCores": "int64",
        "numNonAMContainerPreempted": "int64",
        "numAMContainerPreempted": "int64",
        "preemptedMemorySeconds": "int64",
        "preemptedVcoreSeconds": "int64",
        "preemptedResourceSecondsMap": "object",
        "logAggregationStatus": "object",
        "unmanagedApplication": "bool",
        "amNodeLabelExpression": "object",
        "timeouts": "object",
        "amRPCAddress": "object",
        "type": "object"
    }

    numeric_type_list = ["int", "int32", "int64", "float", "float32", "float64"]
    datetime_type_list = ["datetime64[ns]"]

    for colum in df.columns:

        dtype = df[colum].dtype
        if dtype in numeric_type_list:
            df[colum] = df[colum].apply(lambda record: record if not np.isnan(record) else None)
        elif dtype in datetime_type_list:
            df[colum] = df[colum].apply(lambda record: record if not str(record) == "NaT" else None)
        elif dtype in ("bool", "object"):
            df[colum] = df[colum].apply(lambda record: record if not str(record) == "nan" else None)

    return df


def read_config_files():
    """
    Read all configuration (.json) files related to project
    :return: Schedule details of realtime/ batch pipelines
    """

    pipeline_path = os.path.join(project_dir, "config", "pipeline.json")
    credential_path = os.path.join(project_dir, "config", "credential.json")

    logger.debug(f"Reading credential file")
    credential_dict = Pipeline.read_json(credential_path)
    PagerDutyClient.hostname = credential_dict["pagerduty"]["hostname"]
    PagerDutyClient.token = credential_dict["pagerduty"]["token"]
    PagerDutyClient.routing_key = credential_dict["pagerduty"]["routing_key"]
    YARNClient.hostname = credential_dict["yarn"]["hostname"]
    YARNClient.port_resource_manager = credential_dict["yarn"]["ports"]["resource_manager"]
    YARNClient.port_history_server = credential_dict["yarn"]["ports"]["history_server"]

    logger.debug(f"Reading pipeline configuration")
    schedule_dict: dict = Pipeline.read_json(pipeline_path)

    return schedule_dict


if __name__ == "__main__":
    import urllib3

    # noqa
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 150)

    project_dir = os.path.dirname(os.path.abspath(__file__))
    session = models.session

    logger = Pipeline.setup_logger(
        os.path.join(project_dir, "log", f"monitor-alert-{datetime.datetime.now().strftime('%Y%m%d')}.log")
    )

    column_list = [
        "id", "name", "state", "finalStatus", "startedTime", "launchTime", "finishedTime", "elapsedTime",
        "errorLogPatterns"
    ]

    main()

    session.close()
