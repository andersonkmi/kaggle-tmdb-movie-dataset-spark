package com.iterium.tmbd

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object MovieDataExplorer {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  private def getMovieSchema(colNames: List[String]): StructType = {
    val budgetField =               StructField(colNames(0), LongType, nullable = false)
    val genresField =               StructField(colNames(1), StringType, nullable = false)
    val homepageField =             StructField(colNames(2), StringType, nullable = false)
    val idField =                   StructField(colNames(3), IntegerType, nullable = false)
    val keywordsField =             StructField(colNames(4), StringType, nullable = false)
    val originalLanguageField =     StructField(colNames(5), StringType, nullable = false)
    val originalTitleField =        StructField(colNames(6), StringType, nullable = false)
    val overviewField =             StructField(colNames(7), StringType, nullable = false)
    val popularityField =           StructField(colNames(8), DoubleType, nullable = false)
    val productionCompaniesField =  StructField(colNames(9), StringType, nullable = false)
    val productionCountriesField =  StructField(colNames(10), StringType, nullable = false)
    val releaseDateField =          StructField(colNames(11), StringType, nullable = false)
    val revenueField =              StructField(colNames(12), LongType, nullable = false)
    val runtimeField =              StructField(colNames(13), IntegerType, nullable = false)
    val spokenLanguagesField =      StructField(colNames(14), StringType, nullable = false)
    val statusField =               StructField(colNames(15), StringType, nullable = false)
    val taglineField =              StructField(colNames(16), StringType, nullable = false)
    val titleField =                StructField(colNames(17), StringType, nullable = false)
    val voteAvgField =              StructField(colNames(18), DoubleType, nullable = false)
    val voteCountField =            StructField(colNames(19), LongType, nullable = false)

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
    val headerColumns = contents.first().split("(?:^|,)(?=[^\"]|(\")?|({)?)\"?((?(1)[^\"]*|[^,\"]*))\"?(?=,|$)").toList
    val schema = getMovieSchema(headerColumns)

    val data = contents.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it).map(_.split(",").toList).map(row)
    val dataFrame = sparkSession.createDataFrame(data, schema)
    (headerColumns, dataFrame)
  }
}
