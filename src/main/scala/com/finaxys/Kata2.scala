package com.finaxys

import org.apache.spark.sql.SparkSession

object Kata2 {

  def main(args: Array[String]): Unit = {

    val hadoopPath = System.getProperty("user.dir") + "\\src\\main\\resources\\hadoop";
    System.setProperty("hadoop.home.dir", hadoopPath);

    val spark = SparkSession
      .builder()
      .appName("Spark Kata2")
      .master("local")
      .getOrCreate()

    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .load("./src/main/resources/SalesJan2009.csv")
      .printSchema()

    spark
      .read
      .option("header", "true")
      .json("./src/main/resources/academic_dataset.json")
      .printSchema()
  }
}
