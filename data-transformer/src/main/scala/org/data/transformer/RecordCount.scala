package org.data.transformer


import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object RecordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RecordCount")
      .master("local[*]")
      .getOrCreate()


    val inputPath = if (args.length == 0 || args(0).isEmpty)
      "/Users/adnanrahin/source-code/scala/big-data/spark-rdd-df-comparison-emr/input_data/*"
    else args(0)

    val outputPath = if (args.length < 2 || args(1).isEmpty)
      "/Users/adnanrahin/source-code/scala/big-data/spark-rdd-df-comparison-emr/person_output_data"
    else args(1)

    val df = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(Encoders.product[PersonDomain].schema)
      .csv(inputPath)

    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)


  }

}
