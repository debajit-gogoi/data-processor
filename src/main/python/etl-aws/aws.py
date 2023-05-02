#!/usr/bin/env python3
#
# Copyright (c) 2022 debajit Inc. All rights reserved.
#
# Author: gdowding@debajit.com
#
'''
Program to download cost data from AWS and store into Postgres.
'''
import datetime
import json
import logging
import os
import sys

import pandas as pd
from dateutil import tz
from dateutil import parser as dt_parser

import boto3
from elasticsearch import helpers
from elasticsearch import Elasticsearch

import common


def get_secret(aws_config):
    '''Get secret that can be used to retreive AWS data
    Based on https://us-east-1.console.aws.amazon.com/secretsmanager/secret?name=Organization_Read_RWI&region=us-east-1
    TODO:
    - get initial secret
    - do I need to create a new secret manager with the new secret
    - save current secret before exiting
    '''
    # Create a Secrets Manager client
    session = boto3.session.Session()
    session_token = None
    if aws_config['session_token']:
        session_token = aws_config['session_token']
    client = session.client(
        service_name='secretsmanager',
        aws_access_key_id=aws_config['access_key_id'],
        aws_secret_access_key=aws_config['secret_access_key'],
        aws_session_token=session_token,
        region_name=aws_config['region_name']
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    secret_value_response = client.get_secret_value(SecretId=aws_config['secret_name'])
    secret_value = secret_value_response['SecretString']
    secret_dict = json.loads(secret_value)
    aws_access_key = tuple(secret_dict.keys())[0]
    aws_secret_value = secret_dict[aws_access_key]
    return {
        'key': aws_access_key,
        'value': aws_secret_value,
        'session_token': ''
    }


def create_client(key, secret):
    '''Create AWS cost explorer client.'''
    client = boto3.client('ce',
                          aws_access_key_id=key,
                          aws_secret_access_key=secret)
    return client


def list_accounts(org_client):
    result = org_client.list_accounts(MaxResults=20)
    yield result

    while "NextToken" in result:
        result = org_client.list_accounts(MaxResults=20, NextToken=result["NextToken"])
        yield result


def get_cost_and_usage(client, time_period, granularity="DAILY"):
    '''Iterate over the reports for cost and usage.'''
    response = client.get_cost_and_usage(
        TimePeriod=time_period,
        Granularity=granularity,
        Metrics=['AmortizedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'},
                 {'Type': 'DIMENSION', 'Key': 'SERVICE'}])
    yield response
    while 'NextPageToken' in response:
        response = client.get_cost_and_usage(
            TimePeriod=time_period,
            Granularity=granularity,
            Metrics=['AmortizedCost'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'LINKED_ACCOUNT'},
                     {'Type': 'DIMENSION', 'Key': 'SERVICE'}],
            NextPageToken=response['NextPageToken'])
        yield response


def dt_as_utc(dt):
    '''Convert dt to UTC. If dt is naive, convert it to local timezone first.
    Returns datetime object
    '''
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        # This is a naive datetime object, so set it to local timezone.
        dt = dt.astimezone(tz.tzlocal())
    # Convert to UTC.
    return dt.astimezone(tz.UTC)


def update_arg_parser(argp):
    '''Update argument parser
    Returns ArgumentParser object configured for this program.
    '''
    argp.add_argument('--start-datetime',
                      help='Get data from this datetime inclusive. '
                           'If not provided the start datetime will be current '
                           'datetime - 24 hours.',
                      type=dt_parser.parse)

    argp.add_argument('--end-datetime',
                      help='Get data up to this datetime exclusive. '
                           'If not provided end datetime will be the current datetime.',
                      type=dt_parser.parse)


# Added by AA
def parse_response(response_dict: dict) -> list:
    dimension_dict = {
        record_dict["Value"]: record_dict["Attributes"]["description"] for record_dict in
        response_dict["DimensionValueAttributes"]
        if ("Value" in record_dict and "Attributes" in record_dict)
    }

    usage_list = [
        {
            "startDate": record_list["TimePeriod"]["Start"],
            "endDate": record_list["TimePeriod"]["End"],
            "account": record["Keys"][0],
            "service": record["Keys"][1],
            "description": dimension_dict[record["Keys"][0]],
            "amount": float(record["Metrics"]["AmortizedCost"]["Amount"]),
            "currency": record["Metrics"]["AmortizedCost"]["Unit"]
        }
        for record_list in response_dict["ResultsByTime"]
        for record in record_list["Groups"]
    ]

    return usage_list


def write_to_elastic(data_list, config, index):
    es = Elasticsearch([config["host"]], http_auth=(config["http_auth_user"], config["http_auth_pass"]),
                       scheme=config["scheme"], port=config["port"], use_ssl=True)
    res = helpers.bulk(es, data_list, index=index, chunk_size=1000, request_timeout=600)
    if res:
        logging.info(f"Data written to Elasticsearch index '{index}'")


if __name__ == "__main__":
    argp = common.get_arg_parser()
    update_arg_parser(argp)
    args = argp.parse_args()
    arg_err = common.check_config(args)
    if arg_err:
        argp.print_help()
        sys.exit(1)
    config = common.get_config(args.config)
    common.setup_main(log_level=logging.DEBUG)
    # Silence boto3 log messages except critical
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)

    if args.debug:
        common.setup_main(log_level=logging.DEBUG)
        logging.debug('debug enabled')
    else:
        common.setup_main()
    logging.debug(args)
    aws = config['aws']
    secret = get_secret(aws)

    config['aws']['access_key_id'] = secret['key']
    config['aws']['secret_access_key'] = secret['value']
    config['aws']['session_token'] = secret['session_token']
    with open(args.config, 'w') as fh:
        config.write(fh)

    client = create_client(secret['key'], secret['value'])
    logging.debug(client)
    # Determine start and end datetime.
    if args.end_datetime:
        end_dt = dt_as_utc(args.end_datetime)
    else:
        end_dt = datetime.datetime.now(tz=datetime.timezone.utc)
    if args.start_datetime:
        start_dt = dt_as_utc(args.start_datetime)
    else:
        start_dt = end_dt - datetime.timedelta(days=1)
    # start = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    start = start_dt.strftime('%Y-%m-%d')
    # end = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    end = end_dt.strftime('%Y-%m-%d')
    logging.info('getting cost info from %s upto but not including %s', start, end)
    time_period = {'Start': start, 'End': end}
    # Get cost by linked_account
    # Note: no filter and can't group by recource_id

    """
    AWS Cost and Usage
    """
    combined_list = []
    for response in get_cost_and_usage(client, time_period):
        combined_list.extend(parse_response(response))

    write_to_elastic(combined_list, config["elasticsearch"], index=config["elasticsearch"]["index_aws_usage"])

    if not os.path.exists("data"):
        os.mkdir("data")
    with open(f"data/aws-cost-and-usage-{start.replace('-', '')}-{end.replace('-', '')}.json",
              mode="w", encoding="utf-8") as fp:
        json.dump(combined_list, fp, separators=(",", ":"))

    """
    AWS Account List
    """
    org_client = boto3.client("organizations", aws_access_key_id=secret["key"], aws_secret_access_key=secret["value"])
    account_list = []
    for response in list_accounts(org_client):
        account_list.extend(response["Accounts"])

    account_df = pd.DataFrame(account_list)
    account_df["JoinedTimestamp"] = (account_df["JoinedTimestamp"].dt.tz_convert("UTC")
                                     .dt.strftime("%Y-%m-%d %H:%M:%S"))
    account_list = account_df.to_dict("records")

    write_to_elastic(account_list, config["elasticsearch"], index=config["elasticsearch"]["index_aws_account"])

    with open(f"data/aws-account-list-{start.replace('-', '')}-{end.replace('-', '')}.json",
              mode="w", encoding="utf-8") as fp:
        json.dump(account_list, fp, separators=(",", ":"))
