package fr.datamantra.jobinterview;

import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.WithAssertions;
import org.junit.After;
import org.junit.Before;

public class SparkAssertions implements WithAssertions {

    SparkSession sparkSession = null;

    @Before
    public void setUp() {
        sparkSession = SparkSession
                .builder()
                .appName("Co2SparkSqlApp")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "5")
                .config("spark.ui.enabled", "false")
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate();
    }

    @After
    public void tearDown() {
        sparkSession.stop();
        sparkSession = null;
    }

    String[] topTenResult = new String[]{
            "Qatar - 474.80",
            "Trinidad and Tobago - 338.40",
            "Kuwait - 295.70",
            "Bahrain - 231.30",
            "United Arab Emirates - 216.70",
            "Luxembourg - 214.70",
            "Brunei - 205.00",
            "Saudi Arabia - 177.40",
            "United States - 177.40",
            "Australia - 172.80"
    };

    String[] smallest1950 = new String[]{
            "Nepal - 0.003",
            "Ethiopia - 0.005",
            "Afghanistan - 0.011",
            "Yemen - 0.013",
            "Swaziland - 0.013",
            "Guinea-Bissau - 0.014",
            "Togo - 0.018",
            "Uganda - 0.021",
            "Somalia - 0.021",
            "Bangladesh - 0.026"
    };

    String[] smallest1959 = new String[]{
            "Burkina Faso - 0.002",
            "Nepal - 0.007",
            "Niger - 0.007",
            "Mali - 0.009",
            "Swaziland - 0.011",
            "Ethiopia - 0.016",
            "Chad - 0.021",
            "Guinea-Bissau - 0.024",
            "Somalia - 0.033",
            "Togo - 0.035"
    };

    String[] topFiveResult = new String[]{
            "Bhutan - 1184.39%",
            "Maldives - 479.14%",
            "Seychelles - 389.58%",
            "Cambodia - 281.73%",
            "St. Vincent and the Grenadines - 272.60%"
    };
}
