# etl-data-processor
data-processor

# Idea behind Generic Data Processor

![](/Users/apple/Desktop/Screenshot 2023-05-01 at 10.02.34 PM.png)

# Sample Spark Sumbit Job

```
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
                 --conf spark.yarn.maxAppAttempts=2 \
                 --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
                 --conf spark.dynamicAllocation.enabled=false\
                 --conf spark.yarn.max.executor.failures=8\
                 --conf spark.yarn.executor.failuresValidityInterval=1h \
                 --conf spark.task.maxFailures=8\
                 --jars $LIB_PATH\
                 --driver-class-path $LIB_PATH\
                 --conf spark.executor.extraClassPath=$LIB_PATH\
                 --class com.debajit.dataProcessor.app.RealTimeReadWriteApp\
                 --files /var/lib/hadoop-hdfs/rwi/schema/okta.avsc#schema_location/okta.avsc,/var/lib/hadoop-hdfs/rwi/conf/realtime_debajit_okta_config.json#realtime_debajit_okta_config.json,/var/lib/hadoop-hdfs/rwi/conf/realtime_debajit_okta_enable.json#realtime_debajit_okta_enable.json \
                /var/lib/hadoop-hdfs/rwi/jar/etl-data-processor-1.0-SNAPSHOT.jar schema_location=schema_location
```

# Sample Config Files

Config file

```
{
  "appName": "RealTimeDriver",
  "sessionType": "yarn",
  "process": {
    "OKTA_ACCOUNT": {
      "class": "com.debajit.dataProcessor.processor.realtimeFetch.BasicReal",
      "input": [
        {
          "format": "okta",
          "alias": "okta",
          "url": "https://debajit.okta.com",
          "token": "****",
          "max_minutes_per_trigger": "5",
          "batch_size": "2000",
          "schema": "okta",
          "starting_offset": "2022-08-11T01:00:00Z"
        }
      ],
      "query": "select A.*,substr(A.published, 0, 10) as published_date from okta A",
      "output": [
        {
          "action_class": "com.debajit.dataProcessor.processor.action.WriteToElastic",
          "query": "select *,concat_ws('~~',actor.id,published) as id from #process",
          "wan_only": "true",
          "port": "443",
	  "mapping_id":"id",
          "nodes": "vpc-stage-nextgen-elasticsearch-a6jju4pzbsgmfvuonq3xd2csty.us-east-1.es.amazonaws.com",
          "index": "skiff-nextgen-okta",
          "user": "devops-admin",
          "password": "****",
 	  "ssl":"true",
	  "upsert": "upsert",
          "retryOnConflict": "2",
          "ts_col":"published_date",
          "ts_format":"YYYY-MM-dd"
        },
        {
          "action_class": "com.debajit.dataProcessor.processor.action.WriteToElastic",
          "query":"SELECT actorAlternateId, actorDisplayName, targets.alternateId AS targetAlternateId, targets.displayName As targetDisplayName, published AS published,published_date,concat_ws('~~',actorAlternateId,published) as id FROM (SELECT actor.type AS actorType, actor.alternateId AS actorAlternateId, actor.displayName AS actorDisplayName, published, published_date, targets FROM #process LATERAL VIEW explode(target) target_list AS targets) A WHERE actorType = \"User\" AND targets.type = \"AppInstance\"",
          "wan_only": "true",
          "port": "443",
	  "mapping_id":"id",
          "nodes": "vpc-stage-nextgen-elasticsearch-a6jju4pzbsgmfvuonq3xd2csty.us-east-1.es.amazonaws.com",
          "index": "skiff-nextgen-transform-okta",
          "user": "devops-admin",
          "password": "****",
	  "ssl":"true",
	  "upsert": "upsert",
	  "retryOnConflict": "2",
          "ts_col":"published_date",
          "ts_format":"YYYY-MM-dd"
        }
      ]
    }
  }
}
```

Enable File
```
{
  "process": {
    "actions": {
      "OKTA_ACCOUNT": {
        "isEnabled": "true",
        "properties": {
          "window": "1 minute",
          "checkpoint_dir": "checkpoints/okta_raw"
        }
      }
    }
  }
}
```

=======
