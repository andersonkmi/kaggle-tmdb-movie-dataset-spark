# Kaggle TMDB Movie data exploration in Spark
Spark program for processing data from the TMDB dataset in Scala.

## Introduction

The idea of this project is to play with join operations on data frames inside
Spark and use a different method of loading a CSV file. In the previous project I used
a RDD for loading the information and in this it is being used the format reading
directly.

## Description

This program loads two CSV files obtained from Kaggle: **_tmdb_5000_credits.csv_** and 
**_tmdb_5000_movies.csv_** and performs a join between both data sets.

Other challange using this data set was the mix of CSV and JSON formats and in such
situation it required the use of some special functions to load and handle JSON data.

## Build it and run it

In order to build and run it, extract both CSV files from Kaggle web site (see References section below)
and place the files in the project's root folder, then issue the command below:

```
D:\projects\kaggle-tmdb-movie-dataset-spark> sbt run --destination D:\temp
```

## Exported results

After the program execution, the following folders are created:
- __single_value_df__: this contains a CSV file with single values extracted from the movies data set.
- __sorted_movies_budget__: contains a CSV file with movies sorted by budget.
- __sorted_movies_revenue__: contains a CSV file with movies sorted by revenue.
- __sorted_movies_vote_avg__: contains a CSV file with movies sorted by vote average count.
- __top10_casting_movie_revenue__: contains a JSON file with casting names from top 10 movies by revenue ("most profitable casting").

## References

- [TMDB 5000 Movie Dataset](https://www.kaggle.com/tmdb/tmdb-movie-metadata "TMDB 5000 Movie Dataset")

## Versions
All notable changes to this project will be documented in this section.

### 1.0.0.0 (2018-08-25)
#### Added
- Initial version.
#### Changed
- NA
#### Removed
- NA