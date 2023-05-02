#!/bin/bash
LIB_PATH="$(ls -lrth /var/lib/hadoop-hdfs/rwi/lib/* | awk -F " " '{print $9}' | tr '\n' ',')"
spark-submit --conf 'spark.executor.extraJavaOptions=-Drealtime_config=realtime_debajit_zoom_raw_config.json
                         -Drealtime_enabled=realtime_debajit_zoom_raw_enable.json'\
             --conf 'spark.driver.extraJavaOptions=-Drealtime_config=realtime_debajit_zoom_raw_config.json
                                                 -Drealtime_enabled=realtime_debajit_zoom_raw_enable.json'\
                 --master yarn\
                 --queue default\
                 --name ZOOM_RAW\
                 --num-executors 1\
                 --executor-cores 1\
                 --executor-memory 512m\
                 --driver-memory 512m\
                 --deploy-mode cluster\
                 --jars $LIB_PATH\
                 --conf spark.dynamicAllocation.enabled=false\
                 --driver-class-path $LIB_PATH\
                 --conf spark.executor.extraClassPath=$LIB_PATH\
                 --class com.debajit.dataProcessor.app.RealTimeReadWriteApp\
                 --files /var/lib/hadoop-hdfs/rwi/schema/zoommeeting.avsc#schema_location/zoommeeting.avsc,/var/lib/hadoop-hdfs/rwi/schema/zoomparticipant.avsc#schema_location/zoomparticipant.avsc,/var/lib/hadoop-hdfs/rwi/conf/realtime_debajit_zoom_raw_config.json#realtime_debajit_zoom_raw_config.json,/var/lib/hadoop-hdfs/rwi/conf/realtime_debajit_zoom_raw_enable.json#realtime_debajit_zoom_raw_enable.json \
                /var/lib/hadoop-hdfs/rwi/jar/etl-data-processor-1.0-SNAPSHOT.jar schema_location=schema_location