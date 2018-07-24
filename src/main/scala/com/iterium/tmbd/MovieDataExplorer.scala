package com.iterium.tmbd

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}


object MovieDataExplorer {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  // budget,genres,homepage,id,keywords,original_language,original_title,overview,popularity,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,vote_average,vote_count
  private def getMovieSchema(colNames: List[String]): StructType = {
    
  }
}
