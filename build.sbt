import Dependencies._

organization := "org.sharpsw.spark"

name := "kaggle-tmpdb-movie-dataset-spark"

val appVersion = "1.0.0.0"

val appName = "kaggle-tmpdb-movie-dataset-spark"

version := appVersion

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.354",
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  scalaTest % Test
)