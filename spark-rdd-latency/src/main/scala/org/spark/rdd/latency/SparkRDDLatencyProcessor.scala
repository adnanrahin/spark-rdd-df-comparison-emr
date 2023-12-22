package org.spark.rdd.latency

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.spark.rdd.latency.dataloader.IpCidrCustomDomainUserDataLoader
import org.spark.rdd.latency.datawriter.DataFileWriterLocal
import org.spark.rdd.latency.domain.IpCidrCustomDomainUser
import org.spark.rdd.latency.extract.PersonDataExtract.{countTotalIIDEachState, findAllMalePerson}

object SparkRDDLatencyProcessor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkRDDLatencyProcessor")
      //.master("local[*]") // Comment out if running in local standalone cluster
      .getOrCreate()

    val sc = spark.sparkContext

    val dataSourcePath = args(0)
    val dataPath = args(1)

    val personDomainDataLoader: IpCidrCustomDomainUserDataLoader = new IpCidrCustomDomainUserDataLoader(dataSourcePath, spark)
    val personDomainRDD: RDD[IpCidrCustomDomainUser] = personDomainDataLoader.loadRDD()

    val allMalePerson: DataFrame = {
      findAllMalePerson(personDomainRDD, spark)
    }

    DataFileWriterLocal.dataWriter(dataFrame = allMalePerson,
      dataPath = dataPath,
      directoryName = "spark_rdd_comparison_data")

    val personOnEachState: DataFrame = {
      countTotalIIDEachState(personDomainRDD, spark)
    }

    DataFileWriterLocal.dataWriter(dataFrame = personOnEachState,
      dataPath = dataPath,
      directoryName = "spark_rdd_person_count_each_state")

  }

}
