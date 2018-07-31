package com.iterium.tmdb

import com.iterium.tmdb.MovieDatasetHandler.{extractSingleValuedColumns, readContents}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.iterium.tmdb.utils.Timer.timed

object Main {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Processing Kaggle TMDB movie data information")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-tmdb-movie-spark").master("local[*]").getOrCreate()

    // Loads the first data frame (movies)
    logger.info("Loading the tmdb_5000_movies.csv file")
    val movieDF = timed("Reading tmdb_5000_movies.csv file", readContents("tmdb_5000_movies.csv", sparkSession))

    // Extract single valued columns
    val singleValDF = timed("Extracting single valued columns", extractSingleValuedColumns(movieDF))

    // Loads the second data frame (movie credits)
  }
}
