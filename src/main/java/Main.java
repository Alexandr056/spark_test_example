import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import java.net.URL;

public class Main {

    public static void main(String[] args)  {
        JavaSparkContext jsc = new JavaSparkContext("local", "Java Spark example");
        HiveContext sqlContext = new HiveContext(jsc);

        DataFrame tempByCity = loadCSV(Main.class.getResource("GlobalLandTemperaturesByCity.csv"), sqlContext);
        DataFrame tempByCountry = loadCSV(Main.class.getResource("GlobalLandTemperaturesByCountry.csv"), sqlContext);
        DataFrame globalTemp = loadCSV(Main.class.getResource("GlobalTemperatures.csv"), sqlContext);

        TempCalculations.calculateTempStats(tempByCity, tempByCountry, globalTemp)
                .write()
                .format("parquet")
                .save("result_sets/data.parquet");

        jsc.close();
    }

    public static DataFrame loadCSV(URL source, SQLContext context) {
        return context.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(source.toExternalForm());
    }
}
