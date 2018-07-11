import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;

public class TempCalculations {
    public static DataFrame calculateTempStats(DataFrame tempByCity, DataFrame tempByCountry, DataFrame tempByWorld) {
        DataFrame statsForCities = calculateTempStatsForCities(tempByCity);
        DataFrame statsForCountries = calculateTempStatsForCountries(tempByCountry);
        DataFrame statsForWorld = calculateTempStatsForWorld(tempByWorld);

        return joinTempStats(
                statsForCities,
                statsForCountries,
                statsForWorld
        ).select(
                statsForCities.col("Country"),
                statsForCities.col("City"),
                statsForCities.col("year"),
                statsForCities.col("avg_year").as("CitiesAvgYear"),
                statsForCities.col("min_year").as("CitiesMinYear"),
                statsForCities.col("max_year").as("CitiesMaxYear"),
                statsForCities.col("avg_decade").as("CitiesAvgDecade"),
                statsForCities.col("min_decade").as("CitiesMinDecade"),
                statsForCities.col("max_decade").as("CitiesMaxDecade"),
                statsForCities.col("avg_century").as("CitiesAvgCentury"),
                statsForCities.col("min_century").as("CitiesMinCentury"),
                statsForCities.col("max_century").as("CitiesMaxCentury"),
                statsForCountries.col("avg_year").as("CountriesAvgYear"),
                statsForCountries.col("min_year").as("CountriesMinYear"),
                statsForCountries.col("max_year").as("CountriesMaxYear"),
                statsForCountries.col("avg_decade").as("CountriesAvgDecade"),
                statsForCountries.col("min_decade").as("CountriesMinDecade"),
                statsForCountries.col("max_decade").as("CountriesMaxDecade"),
                statsForCountries.col("avg_century").as("CountriesAvgCentury"),
                statsForCountries.col("min_century").as("CountriesMinCentury"),
                statsForCountries.col("max_century").as("CountriesMaxCentury"),
                statsForWorld.col("avg_year").as("WorldAvgYear"),
                statsForWorld.col("min_year").as("WorldMinYear"),
                statsForWorld.col("max_year").as("WorldMaxYear"),
                statsForWorld.col("avg_decade").as("WorldAvgDecade"),
                statsForWorld.col("min_decade").as("WorldMinDecade"),
                statsForWorld.col("max_decade").as("WorldMaxDecade"),
                statsForWorld.col("avg_century").as("WorldAvgCentury"),
                statsForWorld.col("min_century").as("WorldMinCentury"),
                statsForWorld.col("max_century").as("WorldMaxCentury")
        );
    }

    public static DataFrame joinTempStats(DataFrame statsForCities, DataFrame statsForCountries, DataFrame statsForWorld) {
        return statsForCities
                .join(statsForCountries,
                        statsForCities.col("Country").equalTo(statsForCountries.col("Country"))
                                .and(statsForCities.col("year").equalTo(statsForCountries.col("year"))))
                .join(statsForWorld,statsForCities.col("year").equalTo(statsForWorld.col("year")));
    }

    public static DataFrame calculateTempStatsFor(DataFrame tempData, ImmutableList<Column> groupingBy, String avgTempColumnName) {
        WindowSpec yearWindow = Window.partitionBy(
                Stream.concat(groupingBy.stream(), Stream.of(col("year"))).toArray(Column[]::new));
        WindowSpec decadeWindow = Window.partitionBy(
                Stream.concat(groupingBy.stream(), Stream.of(col("10_years"))).toArray(Column[]::new));
        WindowSpec ageWindow = Window.partitionBy(
                Stream.concat(groupingBy.stream(), Stream.of(col("100_years"))).toArray(Column[]::new));

        return tempData
                .withColumn("date", col("dt").cast("date"))
                .withColumn("year", year(col("date")))
                .withColumn("10_years", year(col("date")).$div(10).cast("int"))
                .withColumn("100_years", year(col("date")).$div(100).cast("int"))
                .withColumn("avg_year", avg(col(avgTempColumnName)).over(yearWindow))
                .withColumn("min_year", min(col(avgTempColumnName)).over(yearWindow))
                .withColumn("max_year", max(col(avgTempColumnName)).over(yearWindow))
                .withColumn("avg_decade", avg(col(avgTempColumnName)).over(decadeWindow))
                .withColumn("min_decade", min(col(avgTempColumnName)).over(decadeWindow))
                .withColumn("max_decade", max(col(avgTempColumnName)).over(decadeWindow))
                .withColumn("avg_century", avg(col(avgTempColumnName)).over(ageWindow))
                .withColumn("min_century", min(col(avgTempColumnName)).over(ageWindow))
                .withColumn("max_century", max(col(avgTempColumnName)).over(ageWindow))
                .groupBy(Stream.concat(groupingBy.stream(), Stream.of(col("year"))).toArray(Column[]::new))
                .agg(first(col("avg_year")).as("avg_year"),
                        first(col("min_year")).as("min_year"),
                        first(col("max_year")).as("max_year"),
                        first(col("avg_decade")).as("avg_decade"),
                        first(col("min_decade")).as("min_decade"),
                        first(col("max_decade")).as("max_decade"),
                        first(col("avg_century")).as("avg_century"),
                        first(col("min_century")).as("min_century"),
                        first(col("max_century")).as("max_century"))
                .orderBy(Stream.concat(groupingBy.stream(), Stream.of(col("year"))).toArray(Column[]::new));

    }

    public static DataFrame calculateTempStatsForWorld(DataFrame tempByWorld){
        return calculateTempStatsFor(tempByWorld, ImmutableList.of(), "LandAverageTemperature");
    }

    public static DataFrame calculateTempStatsForCities(DataFrame tempByCity) {
        return calculateTempStatsFor(tempByCity, ImmutableList.of(col("Country"), col("City")), "AverageTemperature");
    }

    public static DataFrame calculateTempStatsForCountries(DataFrame tempByCountry) {
        return calculateTempStatsFor(tempByCountry, ImmutableList.of(col("Country")), "AverageTemperature");
    }
}
