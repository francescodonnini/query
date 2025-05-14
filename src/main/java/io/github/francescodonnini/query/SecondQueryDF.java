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
        var averages = spark.read().parquet(datasetPath)
                .withColumn("yearMonth", getYearMonth(CsvField.DATETIME_UTC.getName()))
                .select(
                        col("yearMonth"),
                        col(CsvField.COUNTRY.getName()),
                        col(CsvField.CARBON_INTENSITY_DIRECT.getName()),
                        col(CsvField.CFE_PERCENTAGE.getName()))
                .where(col(CsvField.COUNTRY.getName()).equalTo("Italy"))
                .groupBy(col("yearMonth"))
                .agg(avg(CsvField.CARBON_INTENSITY_DIRECT.getName()).as("avgCi"),
                        avg(CsvField.CFE_PERCENTAGE.getName()).as("avgCfe"))
                .orderBy(col("yearMonth"));
        var ciDesc = averages
                .orderBy(col("avgCi").desc())
                .limit(5);
        var ciAsc = averages
                .orderBy(col("avgCi").asc())
                .limit(5);
        var cfeDesc = averages
                .orderBy(col("avgCfe").desc())
                .limit(5);
        var cfeAsc = averages
                .orderBy(col("avgCfe").asc())
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
