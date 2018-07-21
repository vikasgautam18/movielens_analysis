package com.wordpress.technicado.movielens

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import Constants._
import org.apache.log4j.{Level, Logger}

/**
  * This is an RDD based implementation
  */
object SortMoviesByAverageRatingDesc {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    if(args.length != 0){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.SortMoviesByAverageRatingDesc " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar ")
      System.exit(-1)
    }

    val sparkConf = new SparkConf().setAppName("SortMoviesByAverageRatingDesc")
    val sc = new SparkContext(sparkConf)

    val avgRating: RDD[(String, Int)] = new MovieRatingAnalysis(sparkContext = sc).averageMovieRating

    println("Movies sorted by average rating are as follows:: ")
    avgRating.collect.foreach(println)

  }

}
