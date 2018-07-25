package com.iterium.tmbd

import org.apache.log4j.Logger
import org.apache.spark.sql.types._


object MovieDataExplorer {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  // budget,genres,homepage,id,keywords,original_language,original_title,overview,popularity,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,vote_average,vote_count
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
      productionCountriesField)
    StructType(fields)
  }
}
