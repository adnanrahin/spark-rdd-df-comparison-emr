#!/bin/bash

SPARK_HOME=$SPARK_HOME
APP_JAR="/home/rahin/source-code/spark/spark-rdd-df-comparison-emr/data-transformer/target/data-transformer-1.0-SNAPSHOT.jar"
INPUT_PATH="/sandbox/storage/data/person_identity_data/*"
OUTPUT_PATH="/sandbox/storage/data/person_identity_data_parquet/"

$SPARK_HOME/bin/spark-submit \
    --master spark://dev-server01:7077 \
    --deploy-mode cluster \
    --class org.flight.analysis.FlightDataProcessor \
    --name RecordCount \
    --driver-memory 4G \
    --driver-cores 4 \
    --executor-memory 8G \
    --executor-cores 2 \
    --total-executor-cores 12 \
    $APP_JAR $INPUT_PATH $OUTPUT_PATH