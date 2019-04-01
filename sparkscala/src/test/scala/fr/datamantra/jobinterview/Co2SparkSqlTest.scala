package fr.datamantra.jobinterview

import fr.datamantra.jobinterview.model.Results
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class Co2SparkSqlTest extends FunSuite with BeforeAndAfter with Matchers {

  var sparkSession: SparkSession = _

  before {
    sparkSession = SparkSession
      .builder()
      .appName("Co2SparkSqlApp")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "5")
      .config("spark.ui.enabled", "false")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()
  }

  after {
  }

  test("runAppShouldCorrectlyComputeTheTopTenEmittersForLastTenYears") {
    // Test
    val results: Results = Co2SparkSqlApp.runApp(sparkSession, "src/test/resources/co2_emissions_tonnes_per_person.csv")

    // Asserts
    results.topTenEmittersLastTenYears should contain theSameElementsInOrderAs SparkAssertions.topTenResult
  }

  test("runAppShouldCorrectlyComputeTheSmallestTenEmittersForTheFifties") {
    // Test
    val results: Results = Co2SparkSqlApp.runApp(sparkSession, "src/test/resources/co2_emissions_tonnes_per_person.csv")

    // Asserts
    results.smallestEmittersByYear.size should equal(10)
    results.smallestEmittersByYear("1950") should contain theSameElementsInOrderAs SparkAssertions.smallest1950
    results.smallestEmittersByYear("1959") should contain theSameElementsInOrderAs SparkAssertions.smallest1959
  }

  test("runAppShouldCorrectlyComputeTheTopFiveIncreaseFrom1980to2000") {
    // Test
    val results: Results = Co2SparkSqlApp.runApp(sparkSession, "src/test/resources/co2_emissions_tonnes_per_person.csv")

    // Asserts
    results.topFiveIncreaseFrom1980to2000 should contain theSameElementsInOrderAs SparkAssertions.topFiveResult
  }
}
