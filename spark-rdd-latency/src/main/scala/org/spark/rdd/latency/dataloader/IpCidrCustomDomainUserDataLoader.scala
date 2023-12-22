package org.spark.rdd.latency.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.spark.rdd.latency.domain.IpCidrCustomDomainUser

class IpCidrCustomDomainUserDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[IpCidrCustomDomainUser] = {

    val parquetDataFrame = this.spark.read.parquet(filePath)

    val personDomainRDD: RDD[IpCidrCustomDomainUser] = parquetDataFrame.rdd.map(row =>
      IpCidrCustomDomainUser(
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

    personDomainRDD

  }
}