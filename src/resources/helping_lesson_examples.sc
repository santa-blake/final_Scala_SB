
//PRINTING LINES?, printēt rezultātus
// df4.collect.foreach(println)

//  df.limit(10).foreach(row => {
//    row.toSeq.foreach(print)
//    println("")
//  }
//  )

//  df2.where(col("InvoiceNo").equalTo(536365))
// OR    df2.where(expr("InvoiceNo = 536365"))
//    .select("InvoiceNo", "Description", "Total")

// sakarto pec datuma
// val windowSpec = Window
//    .partitionBy("CustomerId", "date")
//    .orderBy(col("Quantity").desc)
//    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
//+
// val meanPurchaseQuantity = mean(col("Quantity")).over(windowSpec)
//  df.where("CustomerId IS NOT NULL").orderBy("CustomerId")
//    .select(
//      col("CustomerId"),
//      ......
//      meanPurchaseQuantity.alias("meanPurchaseQuantity"),
//    .show(50, false)

//ROLLUPI - pa valstim grupejot summas
//  val dfNoNull = df.na.drop()
//  dfNoNull.createOrReplaceTempView("dfNoNull")
//  dfNoNull
//    .groupBy("customerId", "stockCode")
//    .agg("Quantity"->"sum"
//    )
//    .orderBy(desc("customerId"), desc("stockCode"))
//    .show(5,false)

//  //so grouping sets is not available through API in any language
//  //now onto rollups
//  val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
//    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
//    .orderBy("total_quantity", "Date")
//  rolledUpDF.show()

//  //so null in rollup means everything
//  //  dfNoNull.where(expr("Country = null")).show(10,false)
//  //so this is country totals
//  rolledUpDF.where("Date IS NULL").show(10, truncate = false)
//  rolledUpDF.where("Country IS NULL").show(10, truncate = false)
//  dfNoNull.groupBy("Country").agg(sum("Quantity")).show(10, truncate = false)

//  //A cube takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube
//  //does the same thing across all dimensions. This means that it won’t just go by date over the
//  //entire time period, but also the country.
//  dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))
//    .select("Date", "Country", "sum(Quantity)").orderBy("Date").show(15, false)

//  dfNoNull.cube("Date", "Country","InvoiceNo").agg(sum(col("Quantity")))
//    .select("Date", "Country","InvoiceNo", "sum(Quantity)").orderBy(desc("sum(Quantity)"))

//  // in Scala
//  val pivoted = df.groupBy("date").pivot("Country").sum()

//  pivoted.show(25,false)

//  //big pivot with performing aggregration on each value from the country column
//  df.groupBy("date")
//    .pivot("Country")
//    .agg("Quantity"->"sum", "Quantity"->"max","Quantity"->"mean", "Quantity"->"min")

//df0.select(sum(col("volume")),avg(col("volume"))).show()

// renaming columns
// .withColumnRenamed("department","dept.")