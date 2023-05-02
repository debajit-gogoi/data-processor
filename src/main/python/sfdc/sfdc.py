import os
import sys
import json
import datetime
import argparse
import multiprocessing
from itertools import islice
from collections import OrderedDict

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, substring, count, lit, sum, concat, first, lead, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from simple_salesforce import Salesforce, SalesforceLogin, SFType, exceptions

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
BUCKET_NAME = "bucket"
SF_FIELD_TIMESTAMP = "SystemModstamp"


def main(config_dict):
    start_time_str = config_dict["start_time"]
    end_time_str = config_dict["end_time"]
    sf_object_name = config_dict["object_name"]
    sf_field_list_str = config_dict["field_names"]
    sf_column_list = [field.strip() for field in sf_field_list_str.split(",")]
    buffer_size = config_dict["buffer_size"]
    salesforce_dict = config_dict["salesforce"]

    query = generate_query(start_time_str, end_time_str, sf_object_name, SF_FIELD_TIMESTAMP, [SF_FIELD_TIMESTAMP])
    print(f"SOQL: '{query}'")

    sf = sf_connect(salesforce_dict)
    record_list = query_and_poll_sf_records(query, [SF_FIELD_TIMESTAMP], sf)
    query_all(query, [SF_FIELD_TIMESTAMP], sf)

    spark = (SparkSession.builder.appName(f"SFDC-{sf_object_name}-{SF_FIELD_TIMESTAMP}")
             .set("spark.dynamicAllocation.enabled", "false")
             .getOrCreate())

    record_list = write_bucketed_data(record_list, sf_object_name, SF_FIELD_TIMESTAMP, buffer_size, BUCKET_NAME,
                                      end_time_str, spark)

    print("Parallely downloading data")
    download_result = parallel_data_download(sf_object_name, SF_FIELD_TIMESTAMP, sf_column_list, record_list, sf)

    if download_result:
        pass


def query_all(query: str, field_list: list, sf: Salesforce) -> list:
    data_list = sf.query_all(query)
    with open("all-resul.json", mode="w") as fp:
        json.dump(data_list, fp)

    return sf.query_all(query)


def query_and_poll_sf_records(query: str, field_list: list, sf: Salesforce) -> list:
    record_list = []

    print(f"Fetching first batch")
    result_od = sf.query(f"{query}")
    if result_od:
        record_list.extend(
            [tuple([record[field] for field in record.keys() if field in field_list])
             for record in result_od["records"]])

    print(f"Need to fetch additional '{result_od['totalSize'] - len(record_list)}' records")
    while "nextRecordsUrl" in result_od:
        print(f"Fetching batch '{result_od['nextRecordsUrl'].split('/')[-1]}'")
        result_od = sf.query_more(result_od["nextRecordsUrl"], True)
        record_list.extend(
            [tuple([record[field] for field in record.keys() if field in field_list])
             for record in result_od["records"]])

    if result_od["totalSize"] == len(record_list):
        print(f"Total '{result_od['totalSize']}' records fetched successfully")
    else:
        print(f"Could not fetch all the records")

    return record_list


def write_bucketed_data_old(sm_list: list, sf_field_timestamp: str) -> None:
    spark = SparkSession.builder.appName(f"SFDC-{sf_field_timestamp}").getOrCreate()
    schema = StructType([StructField(sf_field_timestamp, StringType())])
    df = spark.createDataFrame(sm_list, schema=schema)

    print(f"Writing data to HDFS")
    (df.write
     .bucketBy(4, sf_field_timestamp)
     .sortBy(sf_field_timestamp)
     .option("path", f"/user/hdfs/sfdc/{sf_field_timestamp.lower()}/")
     .format("parquet")
     .mode("overwrite")
     .saveAsTable(sf_field_timestamp.lower()))

    print(f"Data written to HDFS successfully")


def write_bucketed_data(sm_list: list, sf_table_name: str, sf_field_timestamp: str, buffer_size: int,
                        bucket_name: str, end_time: str, spark: SparkSession) -> list:
    schema = StructType([StructField(sf_field_timestamp, StringType())])

    window_spec = Window.partitionBy(col("buffer_size")).orderBy(col("day"), col("hour"), col("minute"), col("seconds"))

    df = (
        spark.createDataFrame(sm_list, schema=schema)
        .withColumn("day", substring(col(sf_field_timestamp), 1, 10))
        .withColumn("hour", substring(col(sf_field_timestamp), 12, 2))
        .withColumn("minute", substring(col(sf_field_timestamp), 15, 2))
        .withColumn("seconds", substring(col(sf_field_timestamp), 18, 2))
        .groupBy(col("day"), col("hour"), col("minute"), col("seconds"))
        .count()
        .withColumn("buffer_size", lit(buffer_size))
        .withColumn("cumm_sum", F.sum(col("count")).over(window_spec))
        .withColumn(bucket_name, (col("cumm_sum") / col("buffer_size")).cast(IntegerType()))
    )

    print(f"Writing data to HDFS")
    (df.write.option("path", f"/user/hdfs/sfdc/{sf_table_name.lower()}/")
     .mode("overwrite")
     .format("parquet")
     .save(sf_table_name.lower()))
    print(f"Data written to HDFS successfully")

    print(f"Bucketing '{sf_field_timestamp}' field")
    start_window = Window.partitionBy("bucket").orderBy("day", "hour", "minute", "seconds")

    start_df = (
        df.withColumn("start_day", first(col("day")).over(start_window))
        .withColumn("start_hour", first(col("hour")).over(start_window).cast(StringType()))
        .withColumn("start_minute", first(col("minute")).over(start_window).cast(StringType()))
        .withColumn("start_seconds", first(col("seconds")).over(start_window).cast(StringType()))
        .drop("day", "hour", "minute", "seconds", "count", "buffer_size", "cumm_sum")
        .distinct()
        .orderBy("bucket")
    )

    date_df = start_df.withColumn("start_time",
                                  concat(col("start_day"), lit("T"), col("start_hour"), lit(":"), col("start_minute"),
                                         lit(":"), col("start_seconds")))

    lead_window = Window.partitionBy().orderBy("start_time")
    lead_df = date_df.withColumn("end_time", lead(col("start_time")).over(lead_window))

    end_time = end_time.replace("Z", "") if end_time is not None else datetime.datetime.now().strftime(
        "%Y-%m-%dT%H:%M:%S")

    lead_df = lead_df.withColumn("end_time",
                                 when(col("end_time").isNull(), lit(end_time)).otherwise(col("end_time")))

    lead_df = (lead_df.withColumn("start_time", to_timestamp(col("start_time")))
               .withColumn("end_time", to_timestamp(col("end_time"))))

    lead_df.show(100, truncate=False)

    return lead_df.select("bucket", "start_time", "end_time").collect()


def generate_query(start_time: str, end_time: str, table_name: str, field_timestamp: str, field_list: list) -> str:
    query = None

    start_time = to_sf_datetime(datetime.datetime.strptime(start_time, DATETIME_FORMAT))
    field_list_str = ", ".join(field_list)

    if start_time is not None and end_time is not None:
        end_time = to_sf_datetime(datetime.datetime.strptime(end_time, DATETIME_FORMAT))
        query = (f"SELECT {field_list_str} "
                 f"FROM {table_name} "
                 f"WHERE {field_timestamp} >= {start_time} AND {field_timestamp} < {end_time} "
                 f"ORDER BY {field_timestamp} DESC")
    elif start_time is not None:
        query = (f"SELECT {field_list_str} "
                 f"FROM {table_name} "
                 f"WHERE {field_timestamp} >= {start_time} "
                 f"ORDER BY {field_timestamp} DESC")

    return query


def download_data(arg_dict: dict) -> list:
    bucket_id = arg_dict["bucket_id"]
    start_time = arg_dict["start_time"]
    end_time = arg_dict["end_time"]
    table_name = arg_dict["table_name"]
    column_list = arg_dict["column_list_str"]
    field_timestamp = arg_dict["field_timestamp"]
    query = arg_dict["query"]
    sf = arg_dict["sf"]

    print(f"Query bucket '{bucket_id}' ->  '{start_time}' - '{end_time}'")
    print(f"Query: '{query}'")

    return query_and_poll_sf_records(query, column_list, sf)


def parallel_data_download(table_name: str, field_timestamp: str, column_list: list, record_list: list,
                           sf: Salesforce, parallelism: int = 4) -> bool:
    arg_dict_list = [
        {
            "bucket_id": record["bucket"],
            "start_time": record["start_time"].strftime("%Y-%m-%dT%H:%M:%S"),
            "end_time": record["end_time"].strftime("%Y-%m-%dT%H:%M:%S"),
            "table_name": table_name,
            "field_timestamp": field_timestamp,
            "column_list_str": column_list,
            "query": generate_query(record["start_time"].strftime("%Y-%m-%dT%H:%M:%S"),
                                    record["end_time"].strftime("%Y-%m-%dT%H:%M:%S"),
                                    table_name,
                                    field_timestamp,
                                    column_list),
            "sf": sf
        }
        for record in record_list
    ]

    print(f"Started query_all()")
    start_ts = datetime.datetime.now()
    query_all(arg_dict_list[0]["query"], [SF_FIELD_TIMESTAMP], sf)
    end_time = datetime.datetime.now()
    print(f"Finished query_all()")
    print(f"Total time required '{(end_time - start_ts).seconds}' (sec)")

    pool = multiprocessing.Pool(parallelism)
    record_list = pool.map(download_data, arg_dict_list)
    flat_list = [item for sublist in record_list for item in sublist]

    print(f"Writing .json file")
    import json
    with open(f"{table_name.lower()}-{datetime.datetime.now().strftime('%Y%m%dT%H%M%S')}.json", mode="w",
              encoding="utf-8") as fp:
        fp.write(json.dumps(flat_list, separators=(",", ":")))

    return False


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def to_sf_datetime(dt: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)) -> str:
    return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{str(dt.microsecond)[:3].ljust(3, '0')}+0000")


def sf_connect(salesforce_dict: dict) -> Salesforce:
    return Salesforce(username=salesforce_dict["username"], password=salesforce_dict["password"],
                      security_token=salesforce_dict["security_token"], domain=salesforce_dict["domain"])


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Download data from SFDC for a specific time period by bucketing")

    parser.add_argument("--config-file", required=False, help="configuration file name", type=str)
    parser.add_argument("--start-time", required=False, help="start time (inclusive)", type=str)
    parser.add_argument("--end-time", required=False, help="end time (exclusive) - optional", type=str)
    parser.add_argument("--parallelism", required=False, help="parallelism", type=int, default=4)
    parser.add_argument("--buffer-size", required=False, help="buffer size", type=int, default=2000)
    parser.add_argument("--object-name", required=False, help="object name", type=str)
    parser.add_argument("--field-names", required=False, help="field names", type=str)

    args = parser.parse_args()

    if args.config_file is None and (args.start_time is None and args.end_time is None and args.object_name is None
                                     and args.parallelism is None and args.buffer_size is None
                                     and args.field_names is None):
        parser.error("Either command line argument '{--config-file CONFIG_FILE}' or "
                     "'{--start-date START_DATE} [--end-date END-DATE (optional)] {--object-name OBJECT_NAME} "
                     "{--buffer-size BUFFER_SIZE} {--parallelism PARALLELISM} {--field-names FIELD_NAMES}' is required")
    elif args.config_file is None and (
            args.start_time is None or args.end_time is None or args.object_name is None or args.parallelism is None
            or args.buffer_size is None or args.field_names is None):
        parser.error("Not all command line arguments supplied '{--start-date START_DATE} "
                     "[--end-date END-DATE (optional)] {--object-name OBJECT_NAME} {--buffer-size BUFFER_SIZE} "
                     "{--parallelism PARALLELISM} {--field-names FIELD_NAMES}'")

    configuration_dict = {}
    if args.config_file is not None:
        with open(os.path.join("config", args.config_file), mode="r", encoding="utf-8") as fp:
            configuration_dict = json.load(fp)
    else:
        configuration_dict = {
            "start_time": args.start_time,
            "end_time": args.end_time,
            "object_name": args.object_name,
            "parallelism": args.parallelism,
            "buffer_size": args.buffer_size,
            "field_names": args.field_names
        }

    with open(os.path.join("config", "credentials.json"), mode="r", encoding="utf-8") as fp:
        configuration_dict = {**configuration_dict, **json.load(fp)}

    print(f"configuration_dict: '{configuration_dict}'")

    main(configuration_dict)
