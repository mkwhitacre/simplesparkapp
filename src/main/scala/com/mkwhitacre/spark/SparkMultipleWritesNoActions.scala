package com.mkwhitacre.spark

import java.lang.Thread.sleep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkMultipleWritesNoAction {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Counts Multiple Writes No Action"))
    val threshold = args(1).toInt

    // split each document into words
    val tokenized: RDD[String] = sc.textFile(args(0)).flatMap(_.split(" "))

    // count characters
    val chars = tokenized.flatMap(_.toCharArray)
    val charCounts: RDD[(Char, Int)] =  chars.map(charCountSleep(_))

    charCounts.saveAsTextFile("/tmp/spark/char_count_out1")
    charCounts.saveAsTextFile("/tmp/spark/char_count_out2")

//    System.out.println(charCounts.collect().mkString(", "))
  }

  def charCountSleep(c: Char): (Char, Int) = {
    Thread sleep 5000
    (c, 1)
  }

}
