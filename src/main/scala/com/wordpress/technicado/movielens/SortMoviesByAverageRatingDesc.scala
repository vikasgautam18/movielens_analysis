package com.wordpress.technicado.movielens

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This is an RDD based implementation
  */
object SortMoviesByAverageRatingDesc {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.LowestAndHighestAvgRating " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar " +
        "/datasets/movielens/ml-100k/u.data /datasets/movielens/ml-100k/u.item")
      System.exit(-1)
    }

    val sparkConf = new SparkConf().setAppName("SortMoviesByAverageRatingDesc")
    val sc = new SparkContext(sparkConf)

    val inputUData: RDD[String] = sc.textFile(args(0))

    // TODO : work in progress
  }

}
