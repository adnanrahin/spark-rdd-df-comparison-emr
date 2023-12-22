package org.spark.dataframe.latency.dataloader

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.spark.dataframe.latency.domain.IpCidrCustomDomainUser

class IpCidrCustomDomainUserDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadDF(): DataFrame = {

    val personDomainDF: DataFrame = this.spark
      .read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(Encoders.product[IpCidrCustomDomainUser].schema)
      .parquet(filePath)

    personDomainDF
  }
}
