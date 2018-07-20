package com.wordpress.technicado.movielens

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * RDD based approach to find the number of ratings per rating category
  *
  */
object RatingCategories {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length != 2){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.LowestAndHighestAvgRating " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar ")
      System.exit(-1)
    }

    Utils.readConfig("conf/movielens.properties")

    val sparkConf = new SparkConf().setAppName("RatingCategories")
    val sc = new SparkContext(sparkConf)

    val uData: RDD[String] = sc.textFile(Utils.getString("movielens.rating.udata"))

    val y: RDD[(String, Int)] = uData.map(row => {
      val arr = row.split("\t")
      (arr(2), 1)
    })

    val x: RDD[(String, Int)] = y.reduceByKey(_ + _)

    x.foreach(r => println(r._1 + " --> " + r._2))

  }
}
