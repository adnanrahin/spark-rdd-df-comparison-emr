package org.spark.dataframe.latency.extract

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.storage.StorageLevel

object PersonDataExtract {

  def findAllMalePerson(personDomainDF: DataFrame): DataFrame = {
    val maleDF =
      personDomainDF.filter(lower(col("gender")) === "male")
    maleDF.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def countTotalIIDEachState(personDomainDF: DataFrame): DataFrame = {
    val resultDF: DataFrame =
      personDomainDF.groupBy(col("state")).count()
    resultDF.persist(StorageLevel.MEMORY_AND_DISK)
  }

}
