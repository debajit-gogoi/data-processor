#!/bin/bash
LIB_PATH="$(ls -lrth /var/lib/hadoop-hdfs/rwi/lib/* | awk -F " " '{print $9}' | tr '\n' ',')"
spark-submit --conf 'spark.executor.extraJavaOptions=-Dbatch_config=batch_debajit_snowflake_config_test.json -Dbatch_enabled=batch_debajit_snowflake_enable.json' \
  --conf 'spark.driver.extraJavaOptions=-Dbatch_config=batch_debajit_snowflake_config_test.json -Dbatch_enabled=batch_debajit_snowflake_enable.json' \
  --master yarn \
  --name SNOWFLAKE_TEST \
  --queue root.default \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1g \
  --driver-memory 512m \
  --conf spark.dynamicAllocation.enabled=false \
  --deploy-mode cluster \
  --jars $LIB_PATH \
  --driver-class-path $LIB_PATH \
  --conf spark.executor.extraClassPath=$LIB_PATH \
  --class com.debajit.dataProcessor.app.BatchReadWriteApp \
  --files /var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_config_test.json#batch_debajit_snowflake_config_test.json,/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_snowflake_enable.json#batch_debajit_snowflake_enable.json \
  /var/lib/hadoop-hdfs/rwi/jar/etl-data-processor-1.0-SNAPSHOT.jar
