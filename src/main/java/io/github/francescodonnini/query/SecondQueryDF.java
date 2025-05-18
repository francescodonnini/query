package io.github.francescodonnini.query;

import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SecondQueryDF implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;

    public SecondQueryDF(SparkSession spark, String datasetPath, String resultsPath) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
    }

    @Override
    public void close() {
        spark.stop();
    }

    @Override
    public void submit() {
        final var yearMonthCol = "yearMonth";
        final var avgCarbonIntensityCol = "avgCi";
        final var avgCfePercentageCol = "avgCfe";
        var averages = spark.read().parquet(datasetPath + ".parquet")
                .withColumn(yearMonthCol, getYearMonth(CsvField.DATETIME_UTC.getName()))
                .select(
                        col(yearMonthCol),
                        col(CsvField.COUNTRY.getName()),
                        col(CsvField.CARBON_INTENSITY_DIRECT.getName()),
                        col(CsvField.CFE_PERCENTAGE.getName()))
                .where(col(CsvField.COUNTRY.getName()).equalTo("Italy"))
                .groupBy(col(yearMonthCol))
                .agg(avg(CsvField.CARBON_INTENSITY_DIRECT.getName()).as(avgCarbonIntensityCol),
                     avg(CsvField.CFE_PERCENTAGE.getName()).as(avgCfePercentageCol))
                .orderBy(col(yearMonthCol));
        var ciDesc = averages
                .orderBy(col(avgCarbonIntensityCol).desc())
                .limit(5);
        var ciAsc = averages
                .orderBy(col(avgCarbonIntensityCol).asc())
                .limit(5);
        var cfeDesc = averages
                .orderBy(col(avgCfePercentageCol).desc())
                .limit(5);
        var cfeAsc = averages
                .orderBy(col(avgCfePercentageCol).asc())
                .limit(5);
        averages.write()
                .option("header", true)
                .csv(resultsPath + "-plot");
        ciDesc.unionByName(ciAsc)
              .unionByName(cfeDesc)
              .unionByName(cfeAsc)
              .write()
              .option("header", true)
              .csv(resultsPath + "-pairs");
    }

    private Column getYearMonth(String colName) {
        return date_format(to_timestamp(col(colName), CsvField.getDateTimeFormat()), "yyyy-MM");
    }
}
