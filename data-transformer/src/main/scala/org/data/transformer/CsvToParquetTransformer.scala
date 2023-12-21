package org.data.transformer

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvToParquetTransformer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DataTransformer")
      .master("local[*]")
      .getOrCreate()

    val inputData = args(0)
    val outputData = args(1)

    val df = spark.read.option("header", "true").csv(inputData + "*.csv")
    
    dataWriter(df, outputData, "csv_to_parquet_test")

  }

  private final def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + "/" + directoryName

    val rowAsString = dataFrame.rdd.map(_.toSeq.mkString("\t"))

    rowAsString

  }

}
