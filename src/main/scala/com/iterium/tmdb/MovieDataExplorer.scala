package com.iterium.tmdb

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MovieDataExplorer {

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

  private def row(line: List[String]): Row = {
    val list = List(line.head.toLong,
                    line(1),
                    line(2),
                    line(3).toInt,
                    line(4),
                    line(5),
                    line(6),
                    line(7),
                    line(8).toDouble,
                    line(9),
                    line(10),
                    line(11),
                    line(12).toLong,
                    line(13).toInt,
                    line(14),
                    line(15),
                    line(16),
                    line(17),
                    line(18).toDouble,
                    line(19).toLong)
    Row.fromSeq(list)
  }

  def readContents(contents: RDD[String], sparkSession: SparkSession): (List[String], DataFrame) = {
    logger.info("Reading file contents")
    val headerColumns = contents.first().split("(?:^|,)(?=[^\"]|(\")?|(\\{)?)\"?((?(1)[^\"]*|[^,\"]*))\"?(?=,|$)").toList
    val schema = getSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(",").toList).map(row)
    val dataFrame = sparkSession.createDataFrame(data, schema)
    (headerColumns, dataFrame)
  }

  def readContents(file: String, session: SparkSession): DataFrame = {
    session.read.format("com.databricks.spark.csv").schema(getSchema(DefaultColumnNames)).option("header", "true").load(file)
  }
}
