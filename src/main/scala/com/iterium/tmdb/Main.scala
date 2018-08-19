package com.iterium.tmdb

import com.iterium.tmdb.MovieCreditDataSetHandler.sliceDataFrame
import com.iterium.tmdb.MovieDataSetHandler.{extractSingleValuedColumns, getTopMoviesByBudget, getTopMoviesByRevenue, getTopMoviesByVoteAvg, readContents}
import com.iterium.tmdb.utils.DataFrameUtil
import com.iterium.tmdb.utils.DataFrameUtil.saveDataFrameToCsv
import com.iterium.tmdb.utils.FileUtils.buildFilePath
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, types}
import com.iterium.tmdb.utils.Timer.timed
import org.apache.log4j.Level.OFF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache").setLevel(OFF)

    logger.info("Processing Kaggle TMDB movie data information")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-tmdb-movie-spark").master("local[*]").getOrCreate()

    // Loads the first data frame (movies)
    logger.info("Loading the tmdb_5000_movies.csv file")
    val movieDF = timed("Reading tmdb_5000_movies.csv file", readContents("tmdb_5000_movies.csv", sparkSession))

    // Extract single valued columns
    logger.info("Extracting single valued columns from the movie dataset")
    val singleValDF = timed("Extracting single valued columns", extractSingleValuedColumns(movieDF))
    logger.info("Saving current data frame into CSV")
    timed("Saving single values data frame to CSV", saveDataFrameToCsv(singleValDF, buildFilePath("D:\\temp", "single_value_df")))

    logger.info("Extracting top movies by budget")
    val topMoviesByBudget = timed("Extracting top movies by budget", getTopMoviesByBudget(singleValDF))
    logger.info("Saving movies by budget")
    timed("Saving movies by budget", saveDataFrameToCsv(topMoviesByBudget, buildFilePath("D:\\temp", "sorted_movies_budget")))

    logger.info("Listing top movies by revenue")
    val topMoviesByRevenue = timed("Listing top movies by revenue", getTopMoviesByRevenue(singleValDF))
    logger.info("Saving movies by revenue")
    timed("Saving movies by revenue", saveDataFrameToCsv(topMoviesByRevenue, buildFilePath("D:\\temp", "sorted_movies_revenue")))

    logger.info("Listing top movies by vote average")
    val topMoviesByVoteAvg = timed("Listing top movies by vote average", getTopMoviesByVoteAvg(singleValDF))
    logger.info("Saving top movies by vote average")
    timed("Saving top movies by vote average", saveDataFrameToCsv(topMoviesByVoteAvg, buildFilePath("D:\\temp", "sorted_movies_vote_avg")))

    // Loads the second data frame (movie credits)
    logger.info("Loading tmdb_5000_credits.csv file")
    val movieCreditsDF = timed("Reading tmdb_5000_credits.csv file", MovieCreditDataSetHandler.readContents("tmdb_5000_credits.csv", sparkSession))

    logger.info("Slicing credit dataframe")
    val movieCreditsSliced = timed("Slicing credit dataframe", sliceDataFrame(movieCreditsDF))
    val columnNames = Seq("id", "cast")
    val movieCredits = movieCreditsSliced.toDF(columnNames: _*)
    movieCredits.show(10)

    val castIdField = StructField("cast_id", IntegerType)
    val characterField = StructField("character", StringType)
    val creditIdField = StructField("credit_id", StringType)
    val genderField = StructField("gender", IntegerType)
    val idField = StructField("id", IntegerType)
    val nameField = StructField("name", StringType)
    val orderField = StructField("order", IntegerType)

    val fieldList = List(castIdField, characterField, creditIdField, genderField, idField, nameField, orderField)
    val jsonSchema = ArrayType(StructType(fieldList))
    val df = movieCredits.select(col("id"), from_json(col("cast"), jsonSchema).alias("cast"))
    df.printSchema()
    df.show(20)


    //val credits = MovieCreditDataSetHandler.readJsonContents("tmdb_5000_credits_modified.json", sparkSession)
    //credits.printSchema()

    logger.info("Converting the dataframe entirely into json")
    timed("Persisting data frame into json", DataFrameUtil.saveDataFrameToJson(df, buildFilePath("D:\\temp", "movie_credits")))

    // Joining data frames
    val joinedDF = singleValDF.join(movieCredits, Seq("id"))
    joinedDF.show(10)


  }
}
