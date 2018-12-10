package fr.datamantra.jobinterview;

import com.opencsv.CSVParser;
import fr.datamantra.jobinterview.model.Results;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * CO2 Spark Sql Application
 */
public class Co2SparkRDDApp {

    public static void main(String[] args) {

        Co2SparkRDDApp app = new Co2SparkRDDApp();
        String inputFile = args[0];

        SparkConf conf = new SparkConf().setAppName("Co2SparkRDDApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        app.runApp(sc.sc(), inputFile);
    }

    /**
     * Compute all results based on input file
     *
     * @param sparkContext
     * @param inputFile
     * @return
     */
    Results runApp(SparkContext sparkContext, String inputFile) {

        JavaRDD<String> lines = new JavaSparkContext(sparkContext).textFile(inputFile, 5)
                .filter(l -> !l.startsWith("country"));

        // Display a sample of the dataset
        //lines.take(5).forEach(System.out::println);

        List<String> allYears = IntStream.range(1800, 2015)
                .boxed()
                .map(Object::toString)
                .collect(Collectors.toList());

        JavaRDD<Tuple2<String, List<Double>>> values = lines.map(l -> {
                    List<String> cols = Arrays.asList(new CSVParser().parseLine(l));
                    return new Tuple2<>(cols.get(0),
                            cols.subList(1, cols.size()).stream().map(Double::valueOf).collect(Collectors.toList()));
                }
        );

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
