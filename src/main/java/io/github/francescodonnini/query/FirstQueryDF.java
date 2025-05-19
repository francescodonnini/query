package io.github.francescodonnini.query;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Year;
import java.time.ZonedDateTime;
import java.time.Instant;

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
    private static final String COUNTRY_COL_NAME = "country";
    private static final int COUNTRY_COL_INDEX = 0;
    private static final String YEAR_COL_NAME = "year";
    private static final int YEAR_COL_INDEX = 1;
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private final InfluxDbWriterFactory factory;

    public FirstQueryDF(SparkSession spark, String datasetPath, String resultsPath, InfluxDbWriterFactory factory) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
        this.factory = factory;
    }

    @Override
    public void close() {
        spark.stop();
    }

    @Override
    public void submit() {
        final String carbonIntensityCol = "carbonIntensity";
        final String cfePercentageCol = "cfePercentage";
        var df = spark.read().parquet(datasetPath + ".parquet")
             .withColumn(YEAR_COL_NAME, year(to_timestamp(col(CsvField.DATETIME_UTC.getName()), CsvField.getDateTimeFormat())))
             .select(col(YEAR_COL_NAME),
                     col(CsvField.COUNTRY.getName()).as(COUNTRY_COL_NAME),
                     col(CsvField.CARBON_INTENSITY_DIRECT.getName()).as(carbonIntensityCol),
                     col(CsvField.CFE_PERCENTAGE.getName()).as(cfePercentageCol))
             .as(Encoders.bean(Bean.class))
             .groupBy(col(COUNTRY_COL_NAME),
                      col(YEAR_COL_NAME))
             .agg(avg(col(carbonIntensityCol)),
                  min(col(carbonIntensityCol)),
                  max(col(carbonIntensityCol)),
                  avg(col(cfePercentageCol)),
                  max(col(cfePercentageCol)),
                  min(col(cfePercentageCol)));
        df.write()
                .option("header", true)
                .csv(resultsPath);
        df.foreachPartition(partition -> {
            try (var client = factory.create()) {
                var writer = client.getWriteApiBlocking();
                partition.forEachRemaining(row -> writer.writePoint(from(row)));
            }
        });
    }

    private Point from(Row row) {
        return Point.measurement("q1")
                .addTag(COUNTRY_COL_NAME, row.getString(COUNTRY_COL_INDEX))
                .addField("avgCi", row.getDouble(2))
                .addField("minCi", row.getDouble(3))
                .addField("maxCi", row.getDouble(4))
                .addField("avgCfe", row.getDouble(5))
                .addField("minCfe", row.getDouble(6))
                .addField("maxCfe", row.getDouble(7))
                .time(getTime(row), WritePrecision.MS);
    }

    private Instant getTime(Row row) {
        return Year.of(row.getInt(YEAR_COL_INDEX))
                .atDay(1)
                .atStartOfDay()
                .toInstant(ZonedDateTime.now().getOffset());
    }
}
