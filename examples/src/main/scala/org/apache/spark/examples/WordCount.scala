package org.apache.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.master", "local")
    val session = SparkSession.builder().appName("wordCount").getOrCreate()

    val fileRdd: RDD[String] = session.sparkContext.textFile("F:\\github_code\\spark\\examples\\src\\main\\resources\\people.txt")

    val fmapVal = fileRdd.flatMap(x => {
      x.split(",")
    })

    val mapRdd = fmapVal.map(x => (x, 1))

    val res = mapRdd.reduceByKey(_ + _)

    res.collect().foreach(println)

    session.close()
  }
}
