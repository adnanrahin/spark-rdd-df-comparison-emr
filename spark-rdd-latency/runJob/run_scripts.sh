#!/bin/bash

SPARK_HOME=$SPARK_HOME
APP_JAR="/home/rahin/source-code/spark/spark-rdd-df-comparison-emr/spark-rdd-latency/target/spark-rdd-latency-1.0-SNAPSHOT.jar"
INPUT_PATH="/sandbox/storage/data/ip_cidr_data/dataset/ip_cidr_data_parquet/"
OUTPUT_PATH="/sandbox/storage/data/ip_cidr_data/filter_data_local/rdd/"


$SPARK_HOME/bin/spark-submit \
    --master spark://dev-server01:7077 \
    --deploy-mode cluster \
    --class org.spark.rdd.latency.SparkRDDLatencyProcessor \
    --name FlightDataProcessorSpark \
    --driver-memory 4G \
    --driver-cores 4 \
    --executor-memory 8G \
    --executor-cores 2 \
    --total-executor-cores 12 \
    $APP_JAR $INPUT_PATH $OUTPUT_PATH