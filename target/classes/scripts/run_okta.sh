#!/bin/bash
LIB_PATH="$(ls -lrth /var/lib/hadoop-hdfs/rwi/lib/* | awk -F " " '{print $9}' | tr '\n' ',')"
spark-submit --conf 'spark.executor.extraJavaOptions=-Drealtime_config=realtime_debajit_okta_config.json
                         -Drealtime_enabled=realtime_debajit_okta_enable.json'\
             --conf 'spark.driver.extraJavaOptions=-Drealtime_config=realtime_debajit_okta_config.json
                                                 -Drealtime_enabled=realtime_debajit_okta_enable.json'\
                 --master yarn\
                 --name OKTA\
                 --queue default\
                 --num-executors 1\
                 --executor-cores 2\
                 --executor-memory 1g\
                 --driver-memory 1g\
                 --deploy-mode cluster\
                 --conf spark.dynamicAllocation.enabled=false\
                 --jars $LIB_PATH\
                 --driver-class-path $LIB_PATH\
                 --conf spark.executor.extraClassPath=$LIB_PATH\
                 --class com.debajit.dataProcessor.app.RealTimeReadWriteApp\
                 --files /var/lib/hadoop-hdfs/rwi/schema/okta.avsc#schema_location/okta.avsc,/var/lib/hadoop-hdfs/rwi/conf/realtime_debajit_okta_config.json#realtime_debajit_okta_config.json,/var/lib/hadoop-hdfs/rwi/conf/realtime_debajit_okta_enable.json#realtime_debajit_okta_enable.json \
                /var/lib/hadoop-hdfs/rwi/jar/etl-data-processor-1.0-SNAPSHOT.jar schema_location=schema_location