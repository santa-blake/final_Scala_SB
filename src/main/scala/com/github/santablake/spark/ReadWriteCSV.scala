package com.github.santablake.spark

import org.apache.spark.sql.functions.{col, desc, lit, round}
import org.apache.spark.sql.{SparkSession, functions}

object ReadWriteCSV extends App {

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/stock_prices.csv"
  val df0 = session.read
    .format("csv")
    .option("inferSchema", "true") // optional but safer
    .option("header", true) // first row for header
    .load(fPath)
//  df0.printSchema()
//  df0.show()


  // 1. Compute the average daily return of every stock for every date. Print the results to screen
            //  date	      average_return
            //  yyyy-MM-dd	return of all stocks on that date

  val marksColumns1 = Array(col("open"), col("high"), col( "low"), col("close"))
  val averageF = marksColumns1
    .foldLeft(lit(0)){ (x, y) => x+y}/marksColumns1
    .length
  val marksColumns2 = Array(col("volume"), col("Price(Avg)"))
  val returnF = marksColumns2
    .foldLeft(lit(1)){ (x, y) => x*y}

  val df1 = df0
    .orderBy("date")
    .withColumn("Price(Avg)", averageF)
    .withColumn("average_return", returnF)
    .withColumn("Price(Avg)", round(col("Price(Avg)"),2))
    .withColumnRenamed("Price(Avg)","average_price")


  val AverageReturnsByDates = df1
    .select("date", "average_return")
    .groupBy("date")
    .agg(functions.sum("average_return")
      .as("days_avg_trade"))
    .sort("date")
    .show(false)

  val EveryStockDailyCalc = df1
    .select("date", "average_return")
    .groupBy("date")
    .agg(functions.sum("average_return")
      .as("days_avg_trade_by_stock"))

  val DailyAverageReturnsEveryStock = df1
    .select("date", "average_return", "ticker")
    .show()
// ?how to convert this show mode unit to dataframe for saving at same time?


  //2. Save the results to the file as Parquet (CSV and SQL are optional)

//      DariaWriters.writeSingleFile(
//        df = AverageReturnsByDates,
//        format = "parquet",
//        sc = session.sparkContext,
//        tmpFolder = "./src/resources/tmp2",
//        filename = "./src/resources/stocks.parquet"
//      )

         // PREVIOUSLY WRITTEN/SAVED IN .CSV FORMAT, 2 files
//  val tmpPath = "./src/resources/returns.csv"

//  AverageReturnsByDates
//    .coalesce(1)
//    .write.option("header","true")
//    .format("csv")
//    .mode("overwrite")
//    .save(tmpPath)
//  val dir = new File(tmpPath)
//  dir.listFiles.foreach(println)


//  val tmpPath2 = "./src/resources/stocks.csv"

// DailyAverageReturnsEveryStock
//    .coalesce(1)
//    .write.option("header","true")
//    .format("csv")
//    .mode("overwrite")
//    .save(tmpPath2)
//  val dir2 = new File(tmpPath2)
//  dir2.listFiles.foreach(println)

  //  3. Which stock was traded most frequently - as measured by closing price * volume - on average?

  val marksColumns3 = Array(col("volume"), col("close"))
  val averageF2 = marksColumns3.foldLeft(lit(1)){ (x, y) => x*y}
  val df2 = df0
    .withColumn("average_trade", averageF2)
    .withColumnRenamed("ticker","stock_name")
    .sort(desc("average_trade"))

//  val AverageTrades = df2
//    .select("stock_name", "average_trade")
//    .show()

  val TradesByStock = df2
    .groupBy("stock_name")
    .agg(functions.sum("average_trade")
      .as("avg_trade_by_stock"))
    .sort(desc("avg_trade_by_stock"))
    // .show()

}
