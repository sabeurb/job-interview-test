package fr.datamantra.jobinterview

import fr.datamantra.jobinterview.model.Results
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.immutable.TreeMap

object Co2SparkSqlApp {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Co2SparkSqlApp").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    runApp(sparkSession, inputFile)
  }

  def runApp(sparkSession: SparkSession, inputFile: String): Results = {

    val rows: Dataset[Row] = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(inputFile)

    rows.cache()
    // Display a sample of the dataset

    // Compute the top ten emitters for period (2005-2014)
    // Formatted result : decimal part with 2 digits (0.00)

    val firstColumn: String = rows.columns(0) // in ou case equals to country
    val years = List.range(2005, 2015).map(_.toString)

    val cols = rows.columns.filter(years.contains)
    val colswithcountry = firstColumn :: cols.toList

    val sumColumnName = "SUM" // name of the column that will contains the sum foreach country per row
    val finalColumn = "RESULT" // name of the column that will contains the final result
    val rowsWithSum = rows.select(colswithcountry.map(col): _*)
      .withColumn(sumColumnName, cols.map(col).reduce(_ + _))
      .orderBy(desc(sumColumnName))
      .withColumn(sumColumnName, col(sumColumnName).cast(DecimalType(10, 2)))
      .withColumn(finalColumn, concat(col(firstColumn), lit(" - "), col(sumColumnName)))
      .selectExpr(finalColumn)

    val topTen = rowsWithSum.take(10).map(e => e.getAs[String](finalColumn)).toList

    // Compute the smallest ten emitters for each year from 1950 to 1959
    // Ignore countries with 0 emissions
    // Formatted result : decimal part with 3 digits (0.000)
    val fifties = List.range(1950, 1960).map(_.toString)
    val cols15 = rows.columns.filter(fifties.contains)
    val colswithcountry15 = firstColumn :: cols15.toList // all column names [country, 1950..1959]

    var rowsWithSum15 = rows.select(colswithcountry15.map(col): _*)
    val schema = StructType(List(StructField("country", StringType, true),
      StructField("qty", DecimalType(10, 3), true), StructField("year", StringType, true)))

    var df15 = sparkSession.createDataFrame(sparkSession.emptyDataFrame.rdd, schema)
    fifties.foreach(y => {
      val df = rowsWithSum15.select(col(firstColumn), col(y));
      df15 = df15.union(df.withColumn("year", lit(y)))
    })

    df15.filter("qty>0").createOrReplaceTempView("tab15")
    val rankDF = sparkSession.sql("select year, country, qty, " +
      "dense_rank() over (partition by year order by qty asc) as rank " +
      "from tab15").where("rank<11").orderBy(col("year").asc)
      .withColumn("qty", col("qty").cast(DecimalType(10, 3)))

    var smallest: TreeMap[String, List[String]] = TreeMap()
    fifties.foreach(y => {
      var listOfResultPerYear = List[String]()

      rankDF.where(col("year").equalTo(y)).collect.
        map(e => {
          val country = e.getAs[String]("country")
          val qty = e.getAs[java.math.BigDecimal]("qty")
          listOfResultPerYear = s"$country - $qty" :: listOfResultPerYear
          smallest = smallest + (y -> listOfResultPerYear.reverse)
        })
    })

    // Compute the top 5 countries with the biggest increase of emissions from 1980 to 2000
    // Formatted result : decimal part with 2 digits (0.00%)
    var increaseYears = List("country", "1980", "2000")
    var colsAsString = increaseYears.mkString("country,", ",", "").split(",")
    val topFive = rows.select(colsAsString.map(col): _*)
      .withColumn("percentage", (col("2000") - col("1980")).divide(col("1980")))
      .withColumn("percentage", col("percentage").multiply(100).cast(DecimalType(10, 2)))
      .sort(col("percentage").desc)
      .withColumn("result", concat(col("country"), lit(" - "), col("percentage").cast(StringType), lit("%")))
      .selectExpr("result")
      .take(5).map(r => r.getAs[String]("result")).toList

    Results(topTen, smallest, topFive)
  }
}
