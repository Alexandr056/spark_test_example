import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TempCalculationsTest {
    static JavaSparkContext spark;
    static SQLContext sqlContext;

    @BeforeAll
    public static void setUp() {
        spark = new JavaSparkContext("local", "Test context");
        sqlContext = new HiveContext(spark);
    }

    @AfterAll
    public static void tearDown() {
        spark.close();
    }

    @Test
    public void testTempStatCalculationForCities() {
        List<Row> data = Arrays.asList(
                RowFactory.create("1990-01-01", "Russia", "Moscow", 3.0),
                RowFactory.create("1990-02-01", "Russia", "Moscow", 7.0));
        DataFrame testFrame = sqlContext.createDataFrame(data, tempByCitiesSchema);

        assertEquals(TempCalculations.calculateTempStatsForCities(testFrame).collectAsList(), Arrays.asList(
                RowFactory.create("Russia", "Moscow", 1990, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0)
        ));
    }

    @Test
    public void testTempStatCalculationForCountries() {
        List<Row> data = Arrays.asList(
                RowFactory.create("1990-01-01", "Russia", 3.0),
                RowFactory.create("1990-02-01", "Russia", 7.0));
        DataFrame testFrame = sqlContext.createDataFrame(data, tempByCountriesSchema);

        assertEquals(TempCalculations.calculateTempStatsForCountries(testFrame).collectAsList(), Arrays.asList(
                RowFactory.create("Russia", 1990, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0)
        ));
    }

    @Test
    public void testTempCalculationForWorld() {
        List<Row> data = Arrays.asList(
                RowFactory.create("1990-01-01", 3.0),
                RowFactory.create("1990-02-01", 7.0));
        DataFrame testFrame = sqlContext.createDataFrame(data, globalTempSchema);

        assertEquals(TempCalculations.calculateTempStatsForWorld(testFrame).collectAsList(), Arrays.asList(
                RowFactory.create(1990, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0)
        ));
    }

    @Test
    public void testTempCalculating() {
        DataFrame tempByCity = sqlContext.createDataFrame(Arrays.asList(
                RowFactory.create("1990-01-01", "Russia", "Moscow", 3.0),
                RowFactory.create("1990-02-01", "Russia", "Moscow", 7.0)), tempByCitiesSchema);
        DataFrame tempByCountry = sqlContext.createDataFrame(Arrays.asList(
                RowFactory.create("1990-01-01", "Russia", 3.0),
                RowFactory.create("1990-02-01", "Russia", 7.0)), tempByCountriesSchema);
        DataFrame tempByWorld = sqlContext.createDataFrame(Arrays.asList(
                RowFactory.create("1990-01-01", 3.0),
                RowFactory.create("1990-02-01", 7.0)), globalTempSchema);

        DataFrame stats = TempCalculations.calculateTempStats(tempByCity, tempByCountry, tempByWorld);

        assertEquals(stats.collectAsList(), Arrays.asList(
                RowFactory.create("Russia", "Moscow", 1990, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0, 5.0, 3.0, 7.0)
        ));
    }

    StructType tempByCitiesSchema = createStructType(new StructField[] {
            createStructField("dt", StringType, true),
            createStructField("Country", StringType, true),
            createStructField("City", StringType, true),
            createStructField("AverageTemperature", DoubleType, true)
    });

    StructType tempByCountriesSchema = createStructType(new StructField[]{
            createStructField("dt", StringType, true),
            createStructField("Country", StringType, true),
            createStructField("AverageTemperature", DoubleType, true)
    });

    StructType globalTempSchema = createStructType(new StructField[]{
            createStructField("dt", StringType, true),
            createStructField("LandAverageTemperature", DoubleType, true)

    });
}
