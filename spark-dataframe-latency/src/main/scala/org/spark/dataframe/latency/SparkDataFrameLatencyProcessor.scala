package org.spark.dataframe.latency

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.spark.dataframe.latency.dataloader.PersonDomainDataLoader
import org.spark.dataframe.latency.datawriter.DataFileWriterLocal
import org.spark.dataframe.latency.extract.PersonDataExtract.{countTotalIIDEachState, findAllMalePerson}

object SparkDataFrameLatencyProcessor {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkDataFrameLatencyProcessor")
      //.master("local[*]") // Comment out if running in local standalone cluster
      .getOrCreate()

    val sc = spark.sparkContext

    val dataSourcePath = args(0)
    val dataPath = args(1)

    val personDomainDataLoader: PersonDomainDataLoader = new PersonDomainDataLoader(dataSourcePath, spark)
    val personDomainDF: DataFrame = personDomainDataLoader.loadDF()

    val allMalePerson: DataFrame = findAllMalePerson(personDomainDF)

    DataFileWriterLocal.dataWriter(dataFrame = allMalePerson,
      dataPath = dataPath,
      directoryName = "spark_dataframe_comparison_data")

    val statePiiCount = countTotalIIDEachState(personDomainDF)

    DataFileWriterLocal.dataWriter(
      dataFrame = statePiiCount,
      dataPath = dataPath,
      directoryName = "spark_df_person_count_each_state"
    )

  }

}
