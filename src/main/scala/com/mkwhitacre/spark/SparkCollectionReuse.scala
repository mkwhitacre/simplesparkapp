package com.mkwhitacre.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkCollectionReuse {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Counts Reuse Collections"))
    val threshold = args(1).toInt

    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    wordCounts.saveAsTextFile("/tmp/spark/word_count_out")

    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    charCounts.saveAsTextFile("/tmp/spark/char_count_out")

    System.out.println(charCounts.collect().mkString(", "))
  }
}
