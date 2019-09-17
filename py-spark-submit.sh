#!/bin/bash

source config.sh

if [ "$#" -eq 0 ]; then
    echo "Usage: py-spark-submit.sh <cmd_to_run> ..."
    exit
fi

zip -r listenbrainz_spark.zip listenbrainz_spark/
time /usr/local/spark/bin/spark-submit \
    --packages org.apache.spark:spark-avro_2.11:2.4.1 \
    --master $SPARK_URI \
    --conf "spark.scheduler.listenerbus.eventqueue.capacity"=$LISTENERBUS_CAPACITY \
    --conf "spark.cores.max"=$MAX_CORES \
    --conf "spark.executor.cores"=$EXECUTOR_CORES \
    --conf "spark.executor.memory"=$EXECUTOR_MEMORY \
    --conf "spark.driver.memory"=$DRIVER_MEMORY \
    --py-files listenbrainz_spark.zip \
    "$@"
