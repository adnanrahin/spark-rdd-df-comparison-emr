package org.data.transformer


import org.apache.spark.sql.{Encoders, SparkSession}

object RecordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RecordCount")
      .master("local[*]")
      .getOrCreate()


    val df = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(Encoders.product[PersonDomain].schema)
      .csv("/Users/adnanrahin/source-code/scala/big-data/saprk-comparison/csv_to_parquet_test/*")

    df.show(20, truncate = false)

    println(df.count())

  }

}
