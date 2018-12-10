package fr.datamantra.jobinterview;

import fr.datamantra.jobinterview.model.Results;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * CO2 Spark Sql Application
 */
public class Co2SparkSqlApp {

    public static void main(String[] args) {

        Co2SparkSqlApp app = new Co2SparkSqlApp();
        String inputFile = args[0];

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Co2SparkSqlApp")
                .getOrCreate();

        app.runApp(sparkSession, inputFile);
    }

    /**
     * Compute all results based on input file
     *
     * @param sparkSession
     * @param inputFile
     * @return
     */
    Results runApp(SparkSession sparkSession, String inputFile) {

        Dataset<Row> rows = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(inputFile);

        // Display a sample of the dataset
        //rows.show(5);

        List<String> lastTenYears = IntStream.range(2005, 2015)
                .boxed()
                .map(Object::toString)
                .collect(Collectors.toList());

        // Compute the top ten emitters for period (2005-2014)
        // Formatted result : decimal part with 2 digits (0.00)
        List<String> topTen = null;
        // TODO : compute this list

        // Compute the smallest ten emitters for each year from 1950 to 1959
        // Ignore countries with 0 emissions
        // Formatted result : decimal part with 3 digits (0.000)
        List<String> fifties = IntStream.range(1950, 1960)
                .boxed()
                .map(Object::toString)
                .collect(Collectors.toList());

        Map<String, List<String>> smallest = null;
        // TODO : compute this map

        // Compute the top 5 countries with the biggest increase of emissions from 1980 to 2000
        // Formatted result : decimal part with 2 digits (0.00%)
        List<String> topFive = null;
        // TODO : compute this list

        return new Results().withTopTenEmittersLastTenYears(topTen)
                .withSmallestEmittersByYear(smallest)
                .withTopFiveIncreaseFrom1980to2000(topFive);
    }
}
