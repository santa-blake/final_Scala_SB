package com.github.santablake.spark

import org.apache.spark.sql.SparkSession

object DataAnalysisAndPredictions extends App {

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/stock_prices.csv"
  val df0 = session.read
    .format("csv")
    .option("inferSchema", "true") // optional but safer
    .option("header", true) // first row for header
    .load(fPath)


//  val windowSpec = Window
//    .partitionBy("ticker")
//    .orderBy("date"))



}
