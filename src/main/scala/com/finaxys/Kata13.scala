package com.finaxys

import org.apache.spark.sql.{SaveMode, SparkSession}

case class Person(name: String, age: Int)

case class Company(name: String, foundingYear: Int, numEmployees: Int)

object Kata13 {

  def main(args: Array[String]) {

    val hadoopPath = System.getProperty("user.dir") + "\\src\\main\\resources\\hadoop";
    System.setProperty("hadoop.home.dir", hadoopPath);

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Spark Kata13")
      .getOrCreate()


    import sparkSession.implicits._

    val dataset = Seq(1, 2, 3).toDS()
    dataset.show()


    val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
    personDS.show()


    val company = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))


    val companyDF = sparkSession.sparkContext.parallelize(company).toDF()
    val companyDS = companyDF.as[Company]
    companyDS.show()

    val techno = sparkSession.sparkContext.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
    val technoDF = techno.toDF("Id", "Name")
    val technoDS = technoDF.as[(Int, String)]
    technoDS.show()
  }
}
