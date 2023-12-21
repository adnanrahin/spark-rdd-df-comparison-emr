package org.spark.rdd.latency.extract

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.spark.rdd.latency.domain.PersonDomain

object PersonDataExtract {

  private def findAllMalePerson(personDomainRDD: RDD[PersonDomain]): RDD[PersonDomain] = {
    val maleRDD =
      personDomainRDD
        .filter(person => person.gender.equalsIgnoreCase("Male"))
        .persist(StorageLevel.MEMORY_AND_DISK)

    maleRDD
  }

  private def countTotalIIDEachState(personDomainRDD: RDD[PersonDomain]): RDD[(String, Long)] = {

    val resultRdd: RDD[(String, Long)] =
      personDomainRDD
        .groupBy(_.state)
        .mapValues(_.size.toLong)
        .persist(StorageLevel.MEMORY_AND_DISK)

    resultRdd
  }

  def findAllMalePerson(personDomainRDD: RDD[PersonDomain], spark: SparkSession): DataFrame = {

    val mostCancelledAirline = findAllMalePerson(personDomainRDD)

    spark
      .createDataFrame(mostCancelledAirline)
      .toDF

  }

  def countTotalIIDEachState(personDomainRDD: RDD[PersonDomain], spark: SparkSession): DataFrame = {

    val piiIdCount = countTotalIIDEachState(personDomainRDD)

    spark
      .createDataFrame(piiIdCount)
      .toDF("state", "count")

  }

}
