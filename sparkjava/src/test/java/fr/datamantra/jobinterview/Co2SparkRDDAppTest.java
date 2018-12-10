package fr.datamantra.jobinterview;

import fr.datamantra.jobinterview.model.Results;
import org.junit.Test;

public class Co2SparkRDDAppTest extends SparkAssertions {

    @Test
    public void runAppShouldCorrectlyComputeTheTopTenEmittersForLastTenYears() {
        // Setup
        Co2SparkRDDApp app = new Co2SparkRDDApp();

        // Test
        Results results = app.runApp(sparkSession.sparkContext(), "src/test/resources/co2_emissions_tonnes_per_person.csv");


        // Asserts
        assertThat(results.getTopTenEmittersLastTenYears()).containsExactly(topTenResult);
    }

    @Test
    public void runAppShouldCorrectlyComputeTheSmallestTenEmittersForTheFifties() {
        // Setup
        Co2SparkRDDApp app = new Co2SparkRDDApp();

        // Test
        Results results = app.runApp(sparkSession.sparkContext(), "src/test/resources/co2_emissions_tonnes_per_person.csv");

        // Asserts
        assertThat(results.getSmallestEmittersByYear().size()).isEqualTo(10);
        assertThat(results.getSmallestEmittersByYear().get("1950")).containsExactly(smallest1950);
        assertThat(results.getSmallestEmittersByYear().get("1959")).containsExactly(smallest1959);
    }

    @Test
    public void runAppShouldCorrectlyComputeTheTopFiveIncreaseFrom1980to2000() {
        // Setup
        Co2SparkRDDApp app = new Co2SparkRDDApp();

        // Test
        Results results = app.runApp(sparkSession.sparkContext(), "src/test/resources/co2_emissions_tonnes_per_person.csv");


        // Asserts
        assertThat(results.getTopFiveIncreaseFrom1980to2000()).containsExactly(topFiveResult);
    }
}