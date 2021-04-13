package com.finaxys

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Kata1 {

  def main(args: Array[String]) {

    val hadoopPath = System.getProperty("user.dir") + "\\src\\main\\resources\\hadoop";
    System.setProperty("hadoop.home.dir", hadoopPath);

    val spark = SparkSession
      .builder()
      .appName("Spark Kata1")
      .master("local")
      .getOrCreate()

    //    Read Data from CSV file
    val df = spark
      .read
      .option("header", "true")
      .csv("./src/main/resources/SalesJan2009.csv")

    //    1. Calculate Average Price using Dataframes
    //     a) First method (DataFrames)
    df.select(avg(col("Price"))).show()


    //     b) Second Method (Spark SQL)
    //    Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("Sales")

    spark.sql("SELECT avg(Price) FROM Sales").show()

    //    2. Calculate Standard Deviation
    //     a) First method (DataFrames)
    df.groupBy(col("Payment_Type")).agg(stddev(col("Price"))).show()

    //     b) Second method (Spark SQL)
    spark.sql("SELECT Payment_Type, stddev(Price) FROM Sales GROUP BY Payment_Type").show()

    //    3. Bucketing:

    //    Create user defined functions to modify the DataFrame (Modify the Transaction_date colomn: 1/2/09 6:17 is transformed to 617)
    val udfDate: String => Double = _.split(" ")(1).replace(":", "").replace("]", "").toDouble
    val udfPrice: String => Double = _.toDouble

    val uddDate = udf(udfDate)
    val udddPrice = udf(udfPrice)

    //    Apply the User Defined Functions
    val dateFormattedDf  = df.withColumn("Transaction_date",uddDate(col("Transaction_date")))
    val formattedDf  = dateFormattedDf.withColumn("Price",udddPrice(col("Price")))

    //    Create the Bucketizer
    val splits = Array(0.toDouble, 900.toDouble, 1800.toDouble, 2359.toDouble)
    val bucketizer = new Bucketizer()
      .setInputCol("Transaction_date")
      .setOutputCol("Transaction_date_bucketized")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(formattedDf)

    //     a) Bucket according to time range
    bucketedData
      .groupBy("Transaction_date_bucketized", "Product", "Payment_Type")
      .sum("Price")
      .show()

    //     b) Bucket according to Product, time range and payment type
    bucketedData
      .groupBy("Transaction_date_bucketized", "Product", "Payment_Type")
      .sum("Price")
      .show()

  }

}
