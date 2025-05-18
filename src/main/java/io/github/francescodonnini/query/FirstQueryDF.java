package io.github.francescodonnini.query;

import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class FirstQueryDF implements Query {
    private static class Bean {
        private String country;
        private int year;
        private double carbonIntensity;
        private double cfePercentage;

        public double getCarbonIntensity() {
            return carbonIntensity;
        }

        public void setCarbonIntensity(double carbonIntensity) {
            this.carbonIntensity = carbonIntensity;
        }

        public double getCfePercentage() {
            return cfePercentage;
        }

        public void setCfePercentage(double cfePercentage) {
            this.cfePercentage = cfePercentage;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }
    }
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;

    public FirstQueryDF(SparkSession spark, String datasetPath, String resultsPath) {
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
        final String carbonIntensityCol = "carbonIntensity";
        final String cfePercentageCol = "cfePercentage";
        spark.read().parquet(datasetPath + ".parquet")
                .withColumn("year", year(to_timestamp(col(CsvField.DATETIME_UTC.getName()), CsvField.getDateTimeFormat())))
                .select(
                        col("year"),
                        col(CsvField.COUNTRY.getName()).as("country"),
                        col(CsvField.CARBON_INTENSITY_DIRECT.getName()).as(carbonIntensityCol),
                        col(CsvField.CFE_PERCENTAGE.getName()).as(cfePercentageCol)
                )
                .as(Encoders.bean(Bean.class))
                .groupBy(
                        col("country"),
                        col("year"))
                .agg(
                        avg(col(carbonIntensityCol)),
                        min(col(carbonIntensityCol)),
                        max(col(carbonIntensityCol)),
                        avg(col(cfePercentageCol)),
                        max(col(cfePercentageCol)),
                        min(col(cfePercentageCol)))
                .write()
                .csv(resultsPath);
    }
}
