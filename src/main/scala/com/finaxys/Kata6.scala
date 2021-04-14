package com.finaxys

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Kata6 {

  def main(args: Array[String]) {

    val hadoopPath = System.getProperty("user.dir") + "\\src\\main\\resources\\hadoop";
    System.setProperty("hadoop.home.dir", hadoopPath);

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Spark Kata4")
      .getOrCreate()

    val people = sparkSession.read.json("./src/main/resources/people.json")

    people.show(200,false)

    people.printSchema()

  }
}
