# Livy UI

http://rno-ds-hadoop-master-1.corp.debajit.com:8998/ui

# Submit Batch Spark App

- ## cURL Command
    ```bash
    curl -H 'Content-Type: application/json' \ 
         -X POST \
         -i http://rno-ds-hadoop-master-1.corp.debajit.com:8998/batches \
         -d '{"name":"livy-snowflake-test","file":"/user/hdfs/rwi/jar/etl-data-processor-1.0-SNAPSHOT.jar","className":"com.debajit.dataProcessor.app.BatchReadWriteApp","queue":"root.default","numExecutors":1,"executorCores":1,"executorMemory":"1g","driverMemory":"512m","files":["local:////var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_config_test.json","local:////var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_enable.json"],"conf":{"spark.executor.extraJavaOptions":"-Dbatch_config=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_config_test.json -Dbatch_enabled=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_enable.json","spark.driver.extraJavaOptions":"-Dbatch_config=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_config_test.json -Dbatch_enabled=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_enable.json","spark.dynamicAllocation.enabled":false,"spark.executor.extraClassPath":"/user/hdfs/rwi/lib/spark-avro_2.11-2.4.0.jar,/user/hdfs/rwi/lib/spark-sql_2.11-2.4.0.jar,/user/hdfs/rwi/lib/spark-streaming_2.11-2.4.0.jar,/user/hdfs/rwi/lib/spark-core_2.11-2.4.0.jar,/user/hdfs/rwi/lib/okta-sdk-api-8.0.0.jar,/user/hdfs/rwi/lib/okta-sdk-impl-8.0.0.jar,/user/hdfs/rwi/lib/okta-sdk-httpclient-8.0.0.jar,/user/hdfs/rwi/lib/scala-logging_2.11-3.9.0.jar,/user/hdfs/rwi/lib/okta-commons-lang-1.3.0.jar,/user/hdfs/rwi/lib/okta-http-api-1.3.0.jar,/user/hdfs/rwi/lib/okta-http-httpclient-1.3.0.jar,/user/hdfs/rwi/lib/okta-config-check-1.3.0.jar,/user/hdfs/rwi/lib/json-20160212.jar,/user/hdfs/rwi/lib/spark-sql-kafka-0-10_2.11-2.4.0.jar,/user/hdfs/rwi/lib/commons-httpclient-3.1.jar,/user/hdfs/rwi/lib/jjwt-api-0.11.1.jar,/user/hdfs/rwi/lib/jjwt-impl-0.11.1.jar,/user/hdfs/rwi/lib/jjwt-jackson-0.11.1.jar,/user/hdfs/rwi/lib/elasticsearch-spark-20_2.11-7.10.2.jar,/user/hdfs/rwi/lib/snowflake-jdbc-3.13.20.jar,/user/hdfs/rwi/lib/etl-data-processor-1.0-SNAPSHOT.jar"},"jars":["/user/hdfs/rwi/lib/spark-avro_2.11-2.4.0.jar","/user/hdfs/rwi/lib/spark-sql_2.11-2.4.0.jar","/user/hdfs/rwi/lib/spark-streaming_2.11-2.4.0.jar","/user/hdfs/rwi/lib/spark-core_2.11-2.4.0.jar","/user/hdfs/rwi/lib/okta-sdk-api-8.0.0.jar","/user/hdfs/rwi/lib/okta-sdk-impl-8.0.0.jar","/user/hdfs/rwi/lib/okta-sdk-httpclient-8.0.0.jar","/user/hdfs/rwi/lib/scala-logging_2.11-3.9.0.jar","/user/hdfs/rwi/lib/okta-commons-lang-1.3.0.jar","/user/hdfs/rwi/lib/okta-http-api-1.3.0.jar","/user/hdfs/rwi/lib/okta-http-httpclient-1.3.0.jar","/user/hdfs/rwi/lib/okta-config-check-1.3.0.jar","/user/hdfs/rwi/lib/json-20160212.jar","/user/hdfs/rwi/lib/spark-sql-kafka-0-10_2.11-2.4.0.jar","/user/hdfs/rwi/lib/commons-httpclient-3.1.jar","/user/hdfs/rwi/lib/jjwt-api-0.11.1.jar","/user/hdfs/rwi/lib/jjwt-impl-0.11.1.jar","/user/hdfs/rwi/lib/jjwt-jackson-0.11.1.jar","/user/hdfs/rwi/lib/elasticsearch-spark-20_2.11-7.10.2.jar","/user/hdfs/rwi/lib/snowflake-jdbc-3.13.20.jar","/user/hdfs/rwi/lib/etl-data-processor-1.0-SNAPSHOT.jar"]}'
    ```
- ### Payload
    ```json
    {
      "name": "livy-snowflake-test",
      "file": "/user/hdfs/rwi/jar/etl-data-processor-1.0-SNAPSHOT.jar",
      "className": "com.debajit.dataProcessor.app.BatchReadWriteApp",
      "queue": "root.default",
      "numExecutors": 1,
      "executorCores": 1,
      "executorMemory": "1g",
      "driverMemory": "512m",
      "files": [
        "local:////var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_config_test.json",
        "local:////var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_enable.json"
      ],
      "conf": {
        "spark.executor.extraJavaOptions": "-Dbatch_config=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_config_test.json -Dbatch_enabled=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_enable.json",
        "spark.driver.extraJavaOptions": "-Dbatch_config=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_config_test.json -Dbatch_enabled=/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_enable.json",
        "spark.dynamicAllocation.enabled": false,
        "spark.executor.extraClassPath": "/user/hdfs/rwi/lib/spark-avro_2.11-2.4.0.jar,/user/hdfs/rwi/lib/spark-sql_2.11-2.4.0.jar,/user/hdfs/rwi/lib/spark-streaming_2.11-2.4.0.jar,/user/hdfs/rwi/lib/spark-core_2.11-2.4.0.jar,/user/hdfs/rwi/lib/okta-sdk-api-8.0.0.jar,/user/hdfs/rwi/lib/okta-sdk-impl-8.0.0.jar,/user/hdfs/rwi/lib/okta-sdk-httpclient-8.0.0.jar,/user/hdfs/rwi/lib/scala-logging_2.11-3.9.0.jar,/user/hdfs/rwi/lib/okta-commons-lang-1.3.0.jar,/user/hdfs/rwi/lib/okta-http-api-1.3.0.jar,/user/hdfs/rwi/lib/okta-http-httpclient-1.3.0.jar,/user/hdfs/rwi/lib/okta-config-check-1.3.0.jar,/user/hdfs/rwi/lib/json-20160212.jar,/user/hdfs/rwi/lib/spark-sql-kafka-0-10_2.11-2.4.0.jar,/user/hdfs/rwi/lib/commons-httpclient-3.1.jar,/user/hdfs/rwi/lib/jjwt-api-0.11.1.jar,/user/hdfs/rwi/lib/jjwt-impl-0.11.1.jar,/user/hdfs/rwi/lib/jjwt-jackson-0.11.1.jar,/user/hdfs/rwi/lib/elasticsearch-spark-20_2.11-7.10.2.jar,/user/hdfs/rwi/lib/snowflake-jdbc-3.13.20.jar,/user/hdfs/rwi/lib/etl-data-processor-1.0-SNAPSHOT.jar"
      },
      "jars": [
        "/user/hdfs/rwi/lib/spark-avro_2.11-2.4.0.jar",
        "/user/hdfs/rwi/lib/spark-sql_2.11-2.4.0.jar",
        "/user/hdfs/rwi/lib/spark-streaming_2.11-2.4.0.jar",
        "/user/hdfs/rwi/lib/spark-core_2.11-2.4.0.jar",
        "/user/hdfs/rwi/lib/okta-sdk-api-8.0.0.jar",
        "/user/hdfs/rwi/lib/okta-sdk-impl-8.0.0.jar",
        "/user/hdfs/rwi/lib/okta-sdk-httpclient-8.0.0.jar",
        "/user/hdfs/rwi/lib/scala-logging_2.11-3.9.0.jar",
        "/user/hdfs/rwi/lib/okta-commons-lang-1.3.0.jar",
        "/user/hdfs/rwi/lib/okta-http-api-1.3.0.jar",
        "/user/hdfs/rwi/lib/okta-http-httpclient-1.3.0.jar",
        "/user/hdfs/rwi/lib/okta-config-check-1.3.0.jar",
        "/user/hdfs/rwi/lib/json-20160212.jar",
        "/user/hdfs/rwi/lib/spark-sql-kafka-0-10_2.11-2.4.0.jar",
        "/user/hdfs/rwi/lib/commons-httpclient-3.1.jar",
        "/user/hdfs/rwi/lib/jjwt-api-0.11.1.jar",
        "/user/hdfs/rwi/lib/jjwt-impl-0.11.1.jar",
        "/user/hdfs/rwi/lib/jjwt-jackson-0.11.1.jar",
        "/user/hdfs/rwi/lib/elasticsearch-spark-20_2.11-7.10.2.jar",
        "/user/hdfs/rwi/lib/snowflake-jdbc-3.13.20.jar",
        "/user/hdfs/rwi/lib/etl-data-processor-1.0-SNAPSHOT.jar"
      ]
    }
    ```
- #### Fields

| Livy Field | Spark Option | Explanation                                |
|------------|--------------|--------------------------------------------|
| name       |              | The name of this session                   |
| file       |              | File containing the application to execute |
| className  | class        | Class name                                 |
| className  | class        | Class name                                 |

