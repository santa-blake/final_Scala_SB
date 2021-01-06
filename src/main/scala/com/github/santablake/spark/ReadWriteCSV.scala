package com.github.santablake.spark

import com.github.mrpowers.spark.daria.sql.DariaWriters
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

object ReadWriteCSV extends App {

  // Starting a session and loading given .csv file as dataframe, SQL syntax work

  val session = SparkSession.builder().appName("ReadWriteCSV").master("local").getOrCreate()

  println(s"Session started on Spark version ${session.version}")
  val fPath = "./src/resources/stock_prices.csv"
  val df0 = session.read
    .format("csv")
    .option("inferSchema", "true") // optional but safer making columns
    .option("header", true) // first row for header
    .load(fPath)
//  df0.printSchema()
//  df0.show()


  // 1. Computing the average daily return of every stock for every date.
  // // Printing the results to screen in form:
            //  date	      average_return
            //  yyyy-MM-dd	return of all stocks on that date

  val marksColumns1 = Array(col("open"), col("high"), col( "low"), col("close"))
  val averageF = marksColumns1
    .foldLeft(lit(0)){ (x, y) => x+y}/marksColumns1.length
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
    .sort("date") //ordering by date

    AverageReturnsByDates.show(false)

  // bonus calculation (avg trade by stock)
  val EveryStockDailyCalc = df1
    .select("date", "average_return")
    .groupBy(col("date"))
    .agg(functions.sum("average_return")
      .as("days_avg_trade_by_stock"))

  val DailyAverageReturnsEveryStock = df1
    .select("date", "average_return", "ticker")

 DailyAverageReturnsEveryStock.show()


  //2. Save the results to the file as Parquet (CSV and SQL are optional)

  DariaWriters.writeSingleFile(
    df = AverageReturnsByDates,
    format = "parquet",
    sc = session.sparkContext,
    tmpFolder = "./src/resources/tmp1",
    filename = "./src/resources/stocks.parquet"
  )

  DariaWriters.writeSingleFile(
    df = AverageReturnsByDates,
    format = "csv",
    sc = session.sparkContext,
    tmpFolder = "./src/resources/tmp2",
    filename = "./src/resources/stocksDaria.csv"
  )

      // previously written/saved versions in .csv format multi-file folder way

  //val tmpPath = "./src/resources/returns.csv"

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

  val AverageTrades = df2
    .select("stock_name", "average_trade")

  val TradesByStock = df2
    .groupBy("stock_name")
    .agg(functions.sum("average_trade")
      .as("avg_trade_by_stock"))
    .sort(desc("avg_trade_by_stock"))
  TradesByStock.show()

  // or SQL method version, same question
  // saving as SQL dataframe on screen (output SQL file not saved - this to be done by connecting to SQL DB online)

  df0.createOrReplaceTempView("stock_prices_table")

  val BestTradedStock = session.sql( "SELECT ticker, ROUND((SUM(close * volume)/COUNT(volume))/1000,2) AS BestTradedStockReduced1000 " +
    "FROM stock_prices_table " +
    "GROUP BY ticker " +
    "ORDER BY BestTradedStockReduced1000 DESC " +
    "LIMIT 1 ")
  BestTradedStock.show()

  val BestStockTicker = BestTradedStock.select(col("ticker")).first().toString()
  val BestStockValue =  BestTradedStock.select(col("BestTradedStockReduced1000")).first().toString()

  println(s"Calculating most frequently selled stock returns: ${BestStockTicker} with value ${BestStockValue} on average.")


  //  Bonus Question
  // Which stock was the most volatile as measured by annualized standard deviation of daily returns?

  val ByYears = DailyAverageReturnsEveryStock
    .withColumn("year", regexp_extract(col("date"), "\\d{4}", 0))
    .groupBy("ticker", "year")
    .agg(stddev("average_return") * sqrt(count("average_return")))
    .orderBy(desc("(stddev_samp(average_return) * SQRT(count(average_return)))"))
   .withColumnRenamed("(stddev_samp(average_return) * SQRT(count(average_return)))","annual_STD_deviation")
  ByYears.show()

    val VolStockName = ByYears.select(col("ticker")).first().toString()
    val VolStockYear = ByYears.select(col("year")).first().toString()
    val VolStockSD = ByYears.select(col("annual_STD_deviation")).first().toString()

  println(s"Answer is: $VolStockName in $VolStockYear with STDDEV: $VolStockSD .")
}
