#!/bin/env bash

# Project Directory
project_dir='/var/lib/hadoop-hdfs/rwi/etl-aws'

# Config Path
config_path='config.ini'

# Start Date
yesterday=$(date -d 'yesterday' '+%Y-%m-%d')
yesterday_log=${yesterday//-/}

# End Date
today=$(date '+%Y-%m-%d')
today_log=${today//-/}

# Current Timestamp
current_ts=$(date '+%Y%m%dT%H%M%S')

# Log Path
log_path="$project_dir/log/aws-cost-and-usage-$yesterday_log-to-$today_log-at-$current_ts.log"

cd $project_dir || exit

echo "About to run AWS Cost and Usage for '$yesterday' and '$today'"

python3 aws.py --config $config_path --debug --start-datetime "$yesterday" --end-datetime "$today" > "$log_path" 2>&1 &
