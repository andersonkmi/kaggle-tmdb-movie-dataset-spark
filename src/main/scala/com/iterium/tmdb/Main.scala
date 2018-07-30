package com.iterium.tmdb

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.iterium.tmdb.utils.Timer.timed

object Main {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Processing Kaggle TMDB movie data information")

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-tmdb-movie-spark").master("local[*]").getOrCreate()

    val movieDF = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").load("tmdb_5000_movies.csv")
    movieDF.printSchema()
    movieDF.show(10)

    //val movieContents = sparkSession.sparkContext.textFile("tmdb_5000_movies.csv")
    //val (headerColumns, contents) = timed("Reading file contents", MovieDataExplorer.readContents(movieContents, sparkSession))
    //println(headerColumns)
    //contents.cache()

  }
}
