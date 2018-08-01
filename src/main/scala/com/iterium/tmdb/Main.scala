package com.iterium.tmdb

import com.iterium.tmdb.MovieDatasetHandler.{extractSingleValuedColumns, getTopMoviesByBudget, getTopMoviesByRevenue, readContents}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.iterium.tmdb.utils.Timer.timed
import org.apache.log4j.Level.ERROR

object Main {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(ERROR)

    logger.info("Processing Kaggle TMDB movie data information")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-tmdb-movie-spark").master("local[*]").getOrCreate()

    // Loads the first data frame (movies)
    logger.info("Loading the tmdb_5000_movies.csv file")
    val movieDF = timed("Reading tmdb_5000_movies.csv file", readContents("tmdb_5000_movies.csv", sparkSession))

    // Extract single valued columns
    logger.info("Extracting single valued columns from the movie dataset")
    val singleValDF = timed("Extracting single valued columns", extractSingleValuedColumns(movieDF))

    logger.info("Extracting top movies by budget")
    val topMoviesByBudget = timed("Extracting top movies by budget", getTopMoviesByBudget(singleValDF))
    topMoviesByBudget.show(10)

    logger.info("Listing top movies by revenue")
    val topMoviesByRevenue = timed("Listing top movies by revenue", getTopMoviesByRevenue(singleValDF))
    topMoviesByRevenue.show(10)

    // Loads the second data frame (movie credits)
  }
}
