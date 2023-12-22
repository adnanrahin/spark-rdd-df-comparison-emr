package org.spark.rdd.latency.extract

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.spark.rdd.latency.domain.IpCidrCustomDomainUser

object PersonDataExtract {

  private def findAllMalePerson(personDomainRDD: RDD[IpCidrCustomDomainUser]): RDD[IpCidrCustomDomainUser] = {
    val maleRDD =
      personDomainRDD
        .filter(person => person.gender.equalsIgnoreCase("Male"))
        .persist(StorageLevel.MEMORY_AND_DISK)

    maleRDD
  }

  private def countTotalIIDEachState(personDomainRDD: RDD[IpCidrCustomDomainUser]): RDD[(String, Long)] = {

    val resultRdd: RDD[(String, Long)] =
      personDomainRDD
        .groupBy(_.state)
        .mapValues(_.size.toLong)
        .persist(StorageLevel.MEMORY_AND_DISK)

    resultRdd
  }

  def findAllMalePerson(personDomainRDD: RDD[IpCidrCustomDomainUser], spark: SparkSession): DataFrame = {

    val mostCancelledAirline = findAllMalePerson(personDomainRDD)

    spark
      .createDataFrame(mostCancelledAirline)
      .toDF

  }

  def countTotalIIDEachState(personDomainRDD: RDD[IpCidrCustomDomainUser], spark: SparkSession): DataFrame = {

    val piiIdCount = countTotalIIDEachState(personDomainRDD)

    spark
      .createDataFrame(piiIdCount)
      .toDF("state", "count")

  }

}
