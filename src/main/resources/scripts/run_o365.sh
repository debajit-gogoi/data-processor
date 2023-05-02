#!/bin/bash
LIB_PATH="$(ls -lrth /var/lib/hadoop-hdfs/rwi/lib/* | awk -F " " '{print $9}' | tr '\n' ',')"
run_spark_submit (){
echo "Running O365 pipeline for $1"
spark-submit --conf 'spark.executor.extraJavaOptions=-Dbatch_config=batch_debajit_o365_config.json
                         -Dbatch_enabled=batch_debajit_o365_enable.json'\
                 --conf 'spark.driver.extraJavaOptions=-Dbatch_config=batch_debajit_o365_config.json
                                                 -Dbatch_enabled=batch_debajit_o365_enable.json'\
                 --master yarn\
                 --name O365\
                 --queue root.default\
                 --num-executors 1\
                 --executor-cores 1\
                 --executor-memory 512m\
                 --driver-memory 512m\
                 --conf spark.dynamicAllocation.enabled=false\
                 --deploy-mode cluster\
                 --jars $LIB_PATH\
                 --driver-class-path $LIB_PATH\
                 --conf spark.executor.extraClassPath=$LIB_PATH\
                 --class com.debajit.dataProcessor.app.BatchReadWriteApp\
                 --files /var/lib/hadoop-hdfs/rwi/conf/batch_debajit_o365_config.json#batch_debajit_o365_config.json,/var/lib/hadoop-hdfs/rwi/conf/batch_debajit_o365_enable.json#batch_debajit_o365_enable.json \
                /var/lib/hadoop-hdfs/rwi/jar/etl-data-processor-1.0-SNAPSHOT.jar date="$1"
}

if [ "$1" ]
  then
    echo "Date supplied as arguements : "$1
    date=$1
    if [[ $date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]
  		then
        	echo "Date $date is in valid format (YYYY-MM-DD)"
        	echo "Proceeding with pipeline!!"
    else
        	echo "Date $date is in an invalid format (not YYYY-MM-DD)"
        	exit 1
    fi
    run_spark_submit $date
else
	out="$(curl -u 'xxxx:yyyy' -H "Content-Type: application/json" -XGET "https://vpc-stage-nextgen-elasticsearch-a6jju4pzbsgmfvuonq3xd2csty.us-east-1.es.amazonaws.com:443/skiff-nextgen-o365teamuseractivity/_search?size=0&pretty" -d '{"aggs": {"max_date": {"max": {"field":"reportRefreshDate"}}}}' | grep "value_as_string" | xargs | sed -r 's/[value_as_string:]+//g' | xargs)"
	dateToStart=${out%???}
	dateStart=$(date -d @$dateToStart '+%Y-%m-%d')
	echo "Last run date $dateStart"
	dateToEnd=$(date -d "yesterday" '+%Y-%m-%d')
	echo "End date $dateToEnd"
	#$ d=; n=1; until [ "$d" = "$dateToEnd" ]; do ((n++)); d=$(date -d "$dateStart + $n days" +%Y-%m-%d); echo $d; done
	echo "Running for the dates between $dateStart and $dateToEnd"
	d=
	n=0
	until [ "$d" = "$dateToEnd" ]
	do
    		((n++))
    		d=$(date -d "$dateStart + $n days" +%Y-%m-%d)
		run_spark_submit $d
	done
fi