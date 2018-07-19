package com.wordpress.technicado.movielens

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object LowestAndHighestAvgRating {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length != 2){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.LowestAndHighestAvgRating " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar " +
        "/datasets/movielens/ml-100k/u.data /datasets/movielens/ml-100k/u.item")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("SortMoviesByPopularity")
      .getOrCreate()
    import spark.implicits._
    val uDataDF = spark.read.option("delimiter", "\t").csv(args(0))
      .toDF("user_id", "movie_id", "rating", "timestamp")
      .select("movie_id", "rating").withColumn("rating_1", $"rating".cast(IntegerType))

    val highestAvgRatedMovies = uDataDF.groupBy("movie_id").avg("rating_1")
      .toDF("movie_id", "avg_rating")
      .orderBy($"avg_rating".desc)

    val lowestAvgRatedMovies = highestAvgRatedMovies.orderBy($"avg_rating".asc)

    val highestAvgRating = highestAvgRatedMovies.first().getAs[Double]("avg_rating")
    val lowestAvgRating = lowestAvgRatedMovies.first().getAs[Double]("avg_rating")

    val uItem = spark.read.option("delimiter", "|").csv(args(1))
      .select("_c0", "_c1")
      .toDF("movie_id", "movie_name")

    val finalDF = highestAvgRatedMovies.join(uItem, "movie_id")
      .select("movie_name", "avg_rating")

    finalDF.createOrReplaceTempView("movies_avg_rating")

    spark.sql("select movie_name from movies_avg_rating where avg_rating = " + highestAvgRating)
      .toDF("movies_with_highest_avg_rating").show(false)

    spark.sql("select movie_name from movies_avg_rating where avg_rating = " + lowestAvgRating)
      .toDF("movies_with_lowest_avg_rating").show(false)
  }
}
