package org.data.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LoadParquetToRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RecordCount")
      .master("local[*]")
      .getOrCreate()


    val filePath = "/Users/adnanrahin/source-code/scala/big-data/spark-rdd-df-comparison-emr/person_output_data/*"

    val parquetDataFrame = spark.read.parquet(filePath)

    val personDomainRDD: RDD[PersonDomain] = parquetDataFrame.rdd.map(row =>
      PersonDomain(
        row.getAs[String]("firstName"),
        row.getAs[String]("lastName"),
        row.getAs[String]("email"),
        row.getAs[String]("gender"),
        row.getAs[String]("ipV4"),
        row.getAs[String]("ipV6"),
        row.getAs[String]("address"),
        row.getAs[String]("state"),
        row.getAs[String]("city"),
        row.getAs[String]("longitude"),
        row.getAs[String]("latitude"),
        row.getAs[String]("guId"),
        row.getAs[String]("ipV4Cidr"),
        row.getAs[String]("ipV6Cidr")
      )
    )

    personDomainRDD.foreach(println)

  }

}
