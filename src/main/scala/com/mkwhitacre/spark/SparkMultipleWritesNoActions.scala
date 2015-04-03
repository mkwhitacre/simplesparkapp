package com.mkwhitacre.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkMultipleWritesNoAction {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Counts Multiple Writes No Action"))
    val threshold = args(1).toInt

    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    // count characters
    val charCounts = tokenized.flatMap(_.toCharArray).map((_, 1))

    charCounts.saveAsTextFile("/tmp/spark/char_count_out1")
    charCounts.saveAsTextFile("/tmp/spark/char_count_out2")

//    System.out.println(charCounts.collect().mkString(", "))
  }
}
