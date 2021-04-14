package com.finaxys

import org.apache.spark.sql.functions._
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

    val cleanPeople = people.select(
      col("first_name"),
      col("last_name"),
      col("gender"),
      col("age"))


    val columnsNames = cleanPeople.columns
    columnsNames.foreach(println)

    val cleanPeopleWithOld = cleanPeople
      .withColumn("is_old",
        when(col("age") > 65, true)
          .otherwise(false))

    cleanPeopleWithOld.printSchema()

    cleanPeopleWithOld
      .filter(col("is_old"))
      .show(200, false)

    cleanPeopleWithOld
      .groupBy("gender")
      .agg(avg("age"))
      .show(200, false)

    cleanPeopleWithOld.createOrReplaceTempView("people")

    val udfToUpperCase: String => String = _.toUpperCase

    sparkSession.sqlContext.udf.register("toUpperCase", udfToUpperCase)
    val peoplesInUpperCase = sparkSession.sql("SELECT toUpperCase(last_name) as lastNameMaj FROM people")

    peoplesInUpperCase.show(200, false)

    peoplesInUpperCase.write.mode(SaveMode.Overwrite).format("parquet").save("namesAndAges.parquet")

  }
}
