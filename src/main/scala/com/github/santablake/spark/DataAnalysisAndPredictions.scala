package com.github.santablake.spark

import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}

object DataAnalysisAndPredictions extends App {
  //  Big Bonus: Build a model either trying to predict the next day's price(regression).
  //getting and setting DF to analyse

  val session = SparkSession.builder()
    .appName("DataAnalysisAndPredictions")
    .master("local")
    .getOrCreate()
  println(s"Session started on Spark version ${session.version}")

  val fPath = "./src/resources/stock_prices.csv"
  val df = session.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", true)
    .load(fPath)
  df.show()

  //getting previous days prices to compare tem with current
  val windowSpec = Window
    .partitionBy("ticker")
    .orderBy("date")

//adding them to DF preparing DF for regression model, getting free from null value containing rows
  val DFforML = df
    .withColumn("previous_close", lag(col("close"), offset = 1).over(windowSpec))
    .na.drop
  DFforML.show()

//indexing labels, creating and adding metadata(converting strings to numbers) to ticker labels and fitting it to all DF table, transforming it
  val indexer = new StringIndexer()
    .setInputCols(Array("ticker"))
    .setOutputCols(Array("ticker_index"))
    .fit(DFforML)
  val transfDF = indexer.transform(DFforML)

//making all values equal with encoder
  val Encoder = new OneHotEncoder()
    .setInputCols(Array("ticker_index"))
    .setOutputCols(Array("ticker_vector"))
  val encDF = Encoder
    .fit(transfDF)
    .transform(transfDF)

//getting features for analysis
  val VectAssembler = new VectorAssembler()
    .setInputCols(Array("previous_close", "ticker_vector"))
    .setOutputCol("features")
  val DFforMLwithFeatures = VectAssembler.transform(encDF)

  //splitting data in parts for training and testing
  val Array(train, test) = DFforMLwithFeatures.randomSplit(Array(0.7, 0.3), seed = 1989)

  //setting linear regression function to training data set and than performing prediction on test data set part
  val lr = new LinearRegression()
    .setLabelCol("close")
    .setFeaturesCol("features")
  val lrModel = lr.fit(train)
   val lrPredict = lrModel.transform(test)

  println("LinearRegression model to compare close value with predicted result:")
  lrPredict.select( "date", "ticker", "close", "prediction").show(false)

  //Getting data weights feedback to show
  println(s"Testing coefficients: ${lrModel.coefficients}\n Intercept: ${lrModel.intercept}")

  println("Linear regression training summary data information:")
  val trainingSummary = lrModel.summary
  println(s"Total iterations: ${trainingSummary.totalIterations}")
  trainingSummary.residuals.show()
  println(s"RootMean SQ error: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")
  println(s"Mean Abs. Error: ${trainingSummary.meanAbsoluteError}")
  println(s"Explained variance: ${trainingSummary.explainedVariance}")

}

