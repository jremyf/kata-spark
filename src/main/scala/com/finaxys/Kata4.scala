package com.finaxys

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object Kata4 {

  def main(args: Array[String]) {

    val hadoopPath = System.getProperty("user.dir") + "\\src\\main\\resources\\hadoop";
    System.setProperty("hadoop.home.dir", hadoopPath);

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Spark Kata4")
      .getOrCreate()

    val academics = sparkSession.read.json("./src/main/resources/academic_dataset.json")
    val reviews = sparkSession.read.json("./src/main/resources/review_dataset.json")

    academics.printSchema()
    reviews.printSchema()

    val result = reviews.filter(reviews("votes.funny") > 0)
      .join(academics, reviews("business_id").equalTo(academics("business_id")), "inner")
      .selectExpr("name", "review_count", "full_address", "votes", "categories")

    result.write.mode(SaveMode.Overwrite).json("business_with_funny_vote.json")

    val explodeCategory = result.select(explode(col("categories")).as("category"), col("name"))
    explodeCategory.createTempView("explodeCategory")

    sparkSession.sql("SELECT category, count(*) as count FROM explodeCategory group by category").show(200,false)

  }
}
