package fr.datamantra.jobinterview

import com.opencsv.CSVParser
import fr.datamantra.jobinterview.model.Results
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession


object Co2SparkRDDApp {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Co2SparkRDDApp").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    runApp(sparkSession, inputFile)
  }

  def runApp(sparkSession: SparkSession, inputFile: String): Results = {

    val lines = sparkSession.sparkContext.textFile(inputFile, 5)
      .filter(l => !l.startsWith("country"))

    // Display a sample of the dataset
    val allYears = List.range(1800, 2015).map(_.toString)
    val values = lines.map(line => {
      val cols = new CSVParser().parseLine(line).toList
      (cols.head, cols.tail.map(_.toDouble))
    })

    // Compute the top ten emitters for period (2005-2014)
    // Formatted result : decimal part with 2 digits (0.00)
    val top10 = values.mapValues(v => v.slice(2005 - 1800, (2015 - 1800)))
      .mapValues(_.sum).sortBy(_._2, false)
      .take(10)
    val arr = for (v <- top10) yield {
      f"${v._1} - ${v._2}%.2f"
    }
    val topTen: List[String] = arr.toList

    // Compute the smallest ten emitters for each year from 1950 to 1959
    // Ignore countries with 0 emissions
    // Formatted result : decimal part with 3 digits (0.000)
    val fifties = List.range(1950, 1960).map(_.toString)
    val transposeRDD = fifties.map(y => values.map(v => (y, (v._1, v._2(Integer.valueOf(y) - 1800)))) //(year,country,qty)
      .filter(v => v._2._2 > 0))
    val transposedList = transposeRDD.flatMap(rdd => rdd.collect())

    var smallestRDD = sparkSession.sparkContext.parallelize(transposedList)
      .groupByKey.mapValues(x => x.toList.sortBy(_._2).take(10))
      .mapValues(iter => iter.map(e => f"${e._1} - ${e._2}%.3f"))

    val smallest = smallestRDD.collect.toMap

    // Compute the top 5 countries with the biggest increase of emissions from 1980 to 2000
    // Formatted result : decimal part with 2 digits (0.00%)
    val top5 = values.mapValues(v => v(1980 - 1800) :: v(2000 - 1800) :: Nil)
      .filter(_._2(0) > 0).mapValues(v => ((100 * (v(1) - v(0)) / v(0))))
      .sortBy(_._2, false)
      .take(5)

    val arr5 = for (v <- top5) yield {
      f"${v._1} - ${v._2}%.2f%%"
    }
    val topFive: List[String] = arr5.toList

    Results(topTen, smallest, topFive)
  }
}
