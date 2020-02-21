import Dependencies._

organization := "org.sharpsw.spark"

name := "kaggle-tmdb-movie-dataset-spark"

val appVersion = "1.1.2"

val appName = "kaggle-tmdb-movie-dataset-spark"

version := appVersion

scalaVersion := "2.12.10"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.401",
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "org.codecraftlabs.spark" %% "spark-utils" % "1.2.7",
  "org.codecraftlabs.aws" %% "aws-utils" % "1.0.0",
  scalaTest % Test
)