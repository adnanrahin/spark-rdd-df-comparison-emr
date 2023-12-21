package org.data.transformer


import org.apache.spark.sql.{Encoders, SparkSession}

object RecordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RecordCount")
      //.master("local[*]")
      .getOrCreate()


    val df = spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(Encoders.product[PersonDomain].schema)
      .csv("/sandbox/storage/data/person_identity_data/*")

    df.write.parquet("/sandbox/storage/data/person_identity_data/person_output_data")


  }

}
