# Kaggle TMDB Movie data exploration in Spark
Spark program for processing data from the TMDB dataset in Scala.

The idea of this project is to play with join operations on data frames inside
Spark and use a different method of loading a CSV file. In the previous project I used
a RDD for loading the information and in this it is being used the format reading
directly.

## Description

This program loads two CSV files obtained from Kaggle: **_tmdb_5000_credits.csv_** and 
**_tmdb_5000_movies.csv_** and performs a join between both data sets.

## References

- [TMDB 5000 Movie Dataset](https://www.kaggle.com/tmdb/tmdb-movie-metadata "TMDB 5000 Movie Dataset")
