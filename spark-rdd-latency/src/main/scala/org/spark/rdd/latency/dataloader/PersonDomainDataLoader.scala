package org.spark.rdd.latency.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.spark.rdd.latency.domain.PersonDomain

class PersonDomainDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[PersonDomain] = {
    val personDomainCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val personDomainRDD: RDD[PersonDomain] =
      personDomainCSV
        .map(row => row.split("\t", -1))
        .map(str => PersonDomain(
          str(0),
          str(1),
          str(2),
          str(3),
          str(4),
          str(5),
          str(6),
          str(7),
          str(8),
          str(9),
          str(10),
          str(11),
          str(12),
          str(13))
        ).mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }

    personDomainRDD
  }
}
