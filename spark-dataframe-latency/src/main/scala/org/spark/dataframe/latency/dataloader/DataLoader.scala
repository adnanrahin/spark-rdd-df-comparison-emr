package org.spark.dataframe.latency.dataloader

trait DataLoader {
  def loadDF(): Any
}
