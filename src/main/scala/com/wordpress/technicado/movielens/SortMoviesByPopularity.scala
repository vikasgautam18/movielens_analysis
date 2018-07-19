package com.wordpress.technicado.movielens

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object SortMoviesByPopularity {
  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.SortMoviesByPopularity " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar " +
        "/datasets/movielens/ml-100k/u.data /datasets/movielens/ml-100k/u.item spark-examples/SortMoviesByPopularity/output")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("SortMoviesByPopularity")
      .getOrCreate()

    val mRatings = spark.read.option("delimiter", "\t").csv(args(0))
      .toDF("user_id", "movie_id", "rating", "timestamp")
      .select("user_id", "movie_id", "rating")


    val mItem = spark.read.option("delimiter", "|").csv(args(1))
      .select("_c0", "_c1")
      .toDF("movie_id", "movie_name")


    val uData = mRatings.join(mItem, "movie_id").select("movie_name", "rating")
    uData.toDF().createOrReplaceTempView("MovieRatings")
    val sortedDF = spark.sql("select movie_name, count(1) from MovieRatings group by movie_name order by count(1) desc")

    val tsvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
      ("header", "true"))  // Writes a header record with column names

    sortedDF.coalesce(1).write.mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions).csv(args(2))
  }
}
