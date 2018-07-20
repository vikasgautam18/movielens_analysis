package com.wordpress.technicado.movielens

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RatingCategories_withCountByValue {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length != 0){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.RatingCategories_withCountByValue " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar ")
      System.exit(-1)
    }

    Utils.readConfig("conf/movielens.properties")
    val sparkConf = new SparkConf().setAppName("RatingCategories")
    val sc = new SparkContext(sparkConf)

    val uData: RDD[String] = sc.textFile(Utils.getString("movielens.rating.udata"))

    val ratingRDD = uData.map(f => {
      f.split("\t")(2)
    })

    val ratingHistogram: collection.Map[String, Long] = ratingRDD.countByValue()

    println("======== The ratings can be categorized as below =========")
    ratingHistogram.toSeq.foreach(println)
  }
}
