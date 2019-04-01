package fr.datamantra.jobinterview

import com.opencsv.CSVParser
import fr.datamantra.jobinterview.model.Results
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Co2SparkRDDApp {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Co2SparkRDDApp")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    runApp(sparkSession, inputFile)
  }

  def runApp(sparkSession: SparkSession, inputFile: String): Results = {

    val lines = sparkSession.sparkContext.textFile(inputFile, 5)
      .filter(l => !l.startsWith("country"))

    // Display a sample of the dataset
    //lines.take(5).foreach(println)

    val allYears = List.range(1800, 2015).map(_.toString)

    val values = lines.map(line => {
      val cols = new CSVParser().parseLine(line).toList
      (cols.head, cols.tail.map(_.toDouble))
    })

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
