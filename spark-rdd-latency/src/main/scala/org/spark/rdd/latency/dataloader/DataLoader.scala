package org.spark.rdd.latency.dataloader

trait DataLoader {
  def loadRDD(): Any
}
