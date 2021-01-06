package com.github.santablake.spark

import org.apache.spark.sql.SparkSession

object VolatileStocks extends App {

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/stock_prices.csv"
  val df0 = session.read
    .format("csv")
    .option("inferSchema", "true") // optional but safer
    .option("header", true) // first row for header
    .load(fPath)

  //  Bonus Question
  // Which stock was the most volatile as measured by annualized standard deviation of daily returns?



  //  val marksColumns3 = Array(col("volume"), col("close"))
//  val averageF2 = marksColumns3.foldLeft(lit(1)){ (x, y) => x*y}
//  val df2 = df0
//    .withColumn("average_trade", averageF2)
//    .withColumnRenamed("ticker","stock_name")
//    .sort(desc("average_trade"))

//  val windowSpec = Window
//    .partitionBy("ticker")
//    .orderBy("date"))


//  val TradesByStock = df2
//    .select("date", "stock_name", "average_trade")
//    //.select(array_contains(col("date"), "2015-*"))
//    .groupBy("date", "stock_name")
//    .agg(functions.sum("average_trade")
//      .as("avg_trade_by_stock"))
//    .sort(("date"))
//    .show()

    //.groupBy("stock_name",

//  val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
//  val result = wordsDataset
//    .flatMap(_.split(" "))               // Split on whitespace
//    .filter(_ != "")                     // Filter empty words
//    .map(_.toLowerCase())
//    .toDF()                              // Convert to DataFrame to perform aggregation / sorting
//    .groupBy($"value")                   // Count number of occurrences of each word
//    .agg(count("*") as "numOccurances")
//    .orderBy($"numOccurances" desc)      // Show most common words first
//  result.show()

  //TradesByStock.select(stddev(col("avg_trade_by_stock"))).show

}
