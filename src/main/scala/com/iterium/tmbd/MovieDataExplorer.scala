package com.iterium.tmbd

import org.apache.log4j.Logger
import org.apache.spark.sql.types._


object MovieDataExplorer {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  private def getMovieSchema(colNames: List[String]): StructType = {
    val budgetField = StructField(colNames(0), LongType, nullable = false)
    val genresField = StructField(colNames(1), StringType, nullable = false)
    val homepageField = StructField(colNames(2), StringType, nullable = false)
    val idField = StructField(colNames(3), IntegerType, nullable = false)
    val keywordsField = StructField(colNames(4), StringType, nullable = false)
    val originalLanguageField = StructField(colNames(5), StringType, nullable = false)
    val originalTitleField = StructField(colNames(6), StringType, nullable = false)
    val overviewField = StructField(colNames(7), StringType, nullable = false)
    val popularityField = StructField(colNames(8), DoubleType, nullable = false)
    val productionCompaniesField = StructField(colNames(9), StringType, nullable = false)
    val productionCountriesField = StructField(colNames(10), StringType, nullable = false)
    val releaseDateField = StructField(colNames(11), StringType, nullable = false)
    val revenueField = StructField(colNames(12), LongType, nullable = false)
    val runtimeField = StructField(colNames(13), IntegerType, nullable = false)
    val spokenLanguagesField = StructField(colNames(14), StringType, nullable = false)
    val statusField = StructField(colNames(15), StringType, nullable = false)
    val taglineField = StructField(colNames(16), StringType, nullable = false)
    val titleField = StructField(colNames(17), StringType, nullable = false)
    val voteAvgField = StructField(colNames(18), DoubleType, nullable = false)
    val voteCountField = StructField(colNames(19), LongType, nullable = false)

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
}
