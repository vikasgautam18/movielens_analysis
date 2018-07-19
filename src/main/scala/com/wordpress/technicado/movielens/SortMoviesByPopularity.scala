package com.wordpress.technicado.movielens

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object SortMoviesByPopularity {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.SortMoviesByPopularity " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar " +
        "mapreduceexample/movielens/input/u.data mapreduceexample/movielens/output1")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("SortMoviesByPopularity")
      .getOrCreate()
    import spark.implicits._
    val inputDS: Dataset[MovieRating] = spark.read.textFile(args(0)).map(_.split("\t"))
      .map(arr => MovieRating(arr(0), arr(1), arr(2), arr(3)))

    inputDS.toDF().createOrReplaceTempView("MovieRatings")
    val sortedDF = spark.sql("select movieId, count(1) from MovieRatings group by movieId order by count(1) desc")

    val tsvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
      ("header", "true"))  // Writes a header record with column names

    sortedDF.coalesce(1).write.mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions).csv(args(1))
  }
}

case class MovieRating(userId: String, movieId: String, rating: String, timestamp: String)
