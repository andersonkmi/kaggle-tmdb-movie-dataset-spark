package com.iterium.tmdb.utils

import org.apache.spark.sql.DataFrame

object DataFrameUtil {
  def saveDataFrameToCsv(df: DataFrame, destination: String, partitions: Int = 1, saveMode: String = "overwrite", header: Boolean = true): Unit = {
    df.coalesce(partitions).write.mode(saveMode).option("header", header).csv(destination)
  }

  def saveDataFrameToJson(df: DataFrame, destination: String, partitions: Int = 1, saveMode: String = "overwrite", header: Boolean = false): Unit = {
    df.coalesce(partitions).write.mode(saveMode).json(destination)
  }
}
