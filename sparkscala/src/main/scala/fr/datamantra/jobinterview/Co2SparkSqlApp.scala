package fr.datamantra.jobinterview

import fr.datamantra.jobinterview.model.Results
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Co2SparkSqlApp {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Co2SparkSqlApp")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    runApp(sparkSession, inputFile)
  }

  def runApp(sparkSession: SparkSession, inputFile: String): Results = {

    val rows: Dataset[Row] = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(inputFile)

    // Display a sample of the dataset
    //rows.show(5)

    // Compute the top ten emitters for period (2005-2014)
    // Formatted result : decimal part with 2 digits (0.00)
    val topTen = null
    // TODO : compute this list

    // Compute the smallest ten emitters for each year from 1950 to 1959
    // Ignore countries with 0 emissions
    // Formatted result : decimal part with 3 digits (0.000)
    val fifties = List.range(1950, 1960).map(_.toString)

    val smallest = null
    // TODO : compute this map

    // Compute the top 5 countries with the biggest increase of emissions from 1980 to 2000
    // Formatted result : decimal part with 2 digits (0.00%)
    val topFive = null

    Results(topTen, smallest, topFive)
  }
}
