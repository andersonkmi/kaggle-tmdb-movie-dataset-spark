package com.iterium.tmdb

import com.iterium.tmdb.MovieCreditDataSetHandler.{getJsonSchema, sliceDataFrame}
import com.iterium.tmdb.MovieDataSetHandler._
import com.iterium.tmdb.utils.AWSS3Util._
import com.iterium.tmdb.utils.ArgsUtil._
import com.iterium.tmdb.utils.DataFrameUtil.{saveDataFrameToCsv, saveDataFrameToJson}
import com.iterium.tmdb.utils.FileUtils.buildFilePath
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.iterium.tmdb.utils.Timer.{timed, timing}
import com.iterium.tmdb.utils.ZipUtils.unZipIt
import org.apache.log4j.Level.OFF
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {
    val arguments = parseArgs(args)
    val destination = arguments.getOrElse(DestinationDir, ".")
    val source = arguments.getOrElse(SourceDir, ".")
    val s3SourceBucket = arguments.getOrElse(S3SourceBucket, "none")
    val s3SourceKey = arguments.getOrElse(S3SourceKey, "")

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(OFF)

    logger.info("Processing Kaggle TMDB movie data information")

    // Downloads and unzips source file from S3
    if(!s3SourceBucket.equalsIgnoreCase("none")) {
      logger.info("Downloading zip file from S3")
      timed("Downloading zip file from S3", downloadObject(s3SourceBucket, s3SourceKey, source))

      logger.info("Uncompressing zip file")
      timed("Uncompressing zip file", unZipIt(s"$source/$CSVZipFileName", source))
    }

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-tmdb-movie-spark").master("local[*]").getOrCreate()

    // Loads the first data frame (movies)
    logger.info("Loading the tmdb_5000_movies.csv file")
    val movieDF = timed("Reading tmdb_5000_movies.csv file", readContents(buildFilePath(source, TmdbMoviesFileName), sparkSession))

    // Extract single valued columns
    logger.info("Extracting single valued columns from the movie dataset")
    val singleValDF = timed("Extracting single valued columns", extractSingleValuedColumns(movieDF))
    logger.info("Saving current data frame into CSV")
    timed("Saving single values data frame to CSV", saveDataFrameToCsv(singleValDF, buildFilePath(destination, "single_value_df")))

    logger.info("Extracting top movies by budget")
    val topMoviesByBudget = timed("Extracting top movies by budget", getTopMoviesByBudget(singleValDF))
    logger.info("Saving movies by budget")
    timed("Saving movies by budget", saveDataFrameToCsv(topMoviesByBudget, buildFilePath(destination, "sorted_movies_budget")))

    logger.info("Listing top movies by revenue")
    val topMoviesByRevenue = timed("Listing top movies by revenue", getTopMoviesByRevenue(singleValDF))
    logger.info("Saving movies by revenue")
    timed("Saving movies by revenue", saveDataFrameToCsv(topMoviesByRevenue, buildFilePath(destination, "sorted_movies_revenue")))

    logger.info("Listing top movies by vote average")
    val topMoviesByVoteAvg = timed("Listing top movies by vote average", getTopMoviesByVoteAvg(singleValDF))
    logger.info("Saving top movies by vote average")
    timed("Saving top movies by vote average", saveDataFrameToCsv(topMoviesByVoteAvg, buildFilePath(destination, "sorted_movies_vote_avg")))

    // Loads the second data frame (movie credits)
    logger.info("Loading tmdb_5000_credits.csv file")
    val movieCreditsDF = timed("Reading tmdb_5000_credits.csv file", MovieCreditDataSetHandler.readContents(buildFilePath(source, TmdbCreditsFileName), sparkSession))

    logger.info("Slicing credit data frame")
    val movieCreditsSliced = timed("Slicing credit data frame", sliceDataFrame(movieCreditsDF))
    val columnNames = Seq("id", "cast")
    val movieCredits = movieCreditsSliced.toDF(columnNames: _*)

    // Reads an array of json elements
    logger.info("Converting string into json data struct type")
    val movieCreditsMod = timed("Converting string into json data struct type", movieCredits.select(col("id"), from_json(col("cast"), getJsonSchema).alias("cast")))

    // explodes the json array into each movie id
    logger.info("Explodes the nested json cast member list")
    val explodedMovieCredits = timed("Explodes the nested json cast member list", movieCreditsMod.withColumn("cast_member", explode(movieCreditsMod.col("cast"))).drop(col("cast")))

    // Selects the movie id and the cast members names
    logger.info("Selects the movie id and the cast members names")
    val formattedMovieCredits = timed("Selects the movie id and the cast members names", explodedMovieCredits.select(col("id"), col("cast_member.name").alias("name")))

    // Takes the top 10 movies by revenue
    logger.info("Takes the top 10 movies by revenue and retrieves the movie ids")
    val top10MoviesByRevenue = timed("Takes the top 10 movies by revenue", topMoviesByRevenue.take(10))
    val movieIds = top10MoviesByRevenue.map(item => item.get(2)).map(_.toString).map(_.toInt)

    // Lists the cast names associated with the top 10 movies by revenue
    val top10CastingByMovieRevenue = timed("Lists the cast names associated with the top 10 movies by revenue", formattedMovieCredits.filter(col("id") isin (movieIds:_*)).drop("id").dropDuplicates().sort(asc("name")))
    logger.info("Saving top 10 casting by revenue to json")
    timed("Saving top 10 casting by revenue tp json", saveDataFrameToJson(top10CastingByMovieRevenue, buildFilePath(destination, "top10_casting_movie_revenue")))

    println(timing)
  }
}
