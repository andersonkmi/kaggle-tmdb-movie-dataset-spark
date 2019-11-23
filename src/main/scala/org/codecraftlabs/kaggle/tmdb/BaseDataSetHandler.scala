package org.codecraftlabs.kaggle.tmdb

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait BaseDataSetHandler {
  val CSVZipFileName: String = "tmdb-5000-movie-dataset.zip"
  val TmdbMoviesFileName: String = "tmdb_5000_movies.csv"
  val TmdbCreditsFileName: String = "tmdb_5000_credits.csv"

  val ColumnNames: List[String]
  def readContents(file: String, session: SparkSession): DataFrame = {
    session.read.format("com.databricks.spark.csv").schema(getSchema(ColumnNames)).option("header", "true").option("quote", "\"").option("escape", "\"").load(file)
  }

  def getSchema(cols: List[String]): StructType
}
