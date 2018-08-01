package com.iterium.tmdb

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.desc

object MovieDatasetHandler {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val DefaultColumnNames = List("budget","genres","homepage","id","keywords","original_language","original_title","overview","popularity","production_companies","production_countries","release_date","revenue","runtime","spoken_languages","status","tagline","title","vote_average","vote_count")

  def getSchema(colNames: List[String]): StructType = {
    val budgetField =               StructField(colNames(0), LongType, false)
    val genresField =               StructField(colNames(1), StringType, false)
    val homepageField =             StructField(colNames(2), StringType, false)
    val idField =                   StructField(colNames(3), IntegerType, false)
    val keywordsField =             StructField(colNames(4), StringType, false)
    val originalLanguageField =     StructField(colNames(5), StringType, false)
    val originalTitleField =        StructField(colNames(6), StringType, false)
    val overviewField =             StructField(colNames(7), StringType, false)
    val popularityField =           StructField(colNames(8), DoubleType, false)
    val productionCompaniesField =  StructField(colNames(9), StringType, false)
    val productionCountriesField =  StructField(colNames(10), StringType, false)
    val releaseDateField =          StructField(colNames(11), StringType, false)
    val revenueField =              StructField(colNames(12), LongType, false)
    val runtimeField =              StructField(colNames(13), IntegerType, false)
    val spokenLanguagesField =      StructField(colNames(14), StringType, false)
    val statusField =               StructField(colNames(15), StringType, false)
    val taglineField =              StructField(colNames(16), StringType, false)
    val titleField =                StructField(colNames(17), StringType, false)
    val voteAvgField =              StructField(colNames(18), DoubleType, false)
    val voteCountField =            StructField(colNames(19), LongType, false)

    val fields = List(budgetField,
      genresField,
      homepageField,
      idField,
      keywordsField,
      originalLanguageField,
      originalTitleField,
      overviewField,
      popularityField,
      productionCompaniesField,
      productionCountriesField,
      releaseDateField,
      revenueField,
      runtimeField,
      spokenLanguagesField,
      statusField,
      taglineField,
      titleField,
      voteAvgField,
      voteCountField
    )
    StructType(fields)
  }

  def readContents(file: String, session: SparkSession): DataFrame = {
    session.read.format("com.databricks.spark.csv").schema(getSchema(DefaultColumnNames)).option("header", "true").load(file)
  }

  def extractSingleValuedColumns(original: DataFrame): DataFrame = {
    val selectedColumns = Seq("budget", "homepage", "id", "original_title", "popularity", "release_date", "revenue", "runtime", "status", "tagline", "title", "vote_average", "vote_count")
    original.select(selectedColumns.head, selectedColumns.tail: _*)
  }

  def retrieveTopMoviesByBudget(df: DataFrame): DataFrame = {
    df.sort(desc("budget"))
  }
}
