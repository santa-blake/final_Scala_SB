package com.github.santablake.spark

import org.apache.spark.sql.SparkSession

object SparkML extends App {

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/stock_prices.csv"
  val df0 = session.read
    .format("csv")
    .option("inferSchema", "true") // optional but safer
    .option("header", true) // first row for header
    .load(fPath)
  df0.printSchema()
  df0.show()
  df0.summary().show()

//  Bonus Question
//    Which stock was the most volatile as measured by annualized standard deviation of daily returns?
//
//  Big Bonus: Build a model either trying to predict the next day's price(regression) or simple UP/DOWN/UNCHANGED? classificator. You can only use information information from earlier dates.
//
//    You can use other information if you wish from other sources for this predictor, the only thing what you can not do is use future data. :)
//
//  One idea would be to find and extract some information about particular stock and columns with industry data (for example APPL would be music,computers,mobile)
//
//  Do not expect something with high accuracy but main task is getting something going.
//
//    Good extra feature would be for your program to read any .CSV in this particular format and try to do the task. This means your program would accept program line parameters.
//
//    Assumptions
//
//  No dividends adjustments are necessary, using only the closing price to determine returns
//    If a price is missing on a given date, you can compute returns from the closest available date
//  Return can be trivially computed as the % difference of two prices
//
//
//  Evaluation
//  Correctness is most important
//    Elegance and style is the next most important
//    Performance is the last criteria, but can be detrimental to you if your implementation is unreasonably costly
//
//  Some helpful hints
//  You may or may not need these hints depending on the approach you take
//
//
//  Window Functions on DataFrame https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html


}
