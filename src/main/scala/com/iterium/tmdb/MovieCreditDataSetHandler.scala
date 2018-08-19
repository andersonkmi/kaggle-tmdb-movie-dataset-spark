package com.iterium.tmdb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MovieCreditDataSetHandler {
  private val ColumnNames = List("movie_id", "title", "cast", "crew")

  private def getSchema(colNames: List[String]): StructType = {
    val movieIdField  = StructField(colNames(0), IntegerType, nullable = false)
    val titleField    = StructField(colNames(1), StringType, nullable = false)
    val castField     = StructField(colNames(2), StringType, nullable = false)
    val crewField     = StructField(colNames(3), StringType, nullable = false)

    val fields = List(movieIdField, titleField, castField, crewField)
    StructType(fields)
  }

  def readContents(file: String, session: SparkSession): DataFrame = {
    session.read.format("com.databricks.spark.csv").schema(getSchema(ColumnNames)).option("header", "true").option("quote", "\"").option("escape", "\"").load(file)
  }

  def sliceDataFrame(df: DataFrame): DataFrame = {
    val selectedColumns = List("movie_id", "cast")
    df.select(selectedColumns.head, selectedColumns.tail: _*)
  }

  def readJsonContents(file: String, session: SparkSession): DataFrame = {
    session.read.json(file)
  }
}
