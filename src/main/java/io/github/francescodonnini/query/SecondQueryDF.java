package io.github.francescodonnini.query;

import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.ArrayList;

import static org.apache.spark.sql.functions.*;

public class SecondQueryDF implements Query {
    private static final String YEAR_MONTH_COL_NAME = "yearMonth";
    private static final int    YEAR_MONTH_COL_INDEX = 0;
    private static final String AVG_CARBON_INTENSITY_COL_NAME = "avgCi";
    private static final int    AVG_CARBON_INTENSITY_COL_INDEX = 1;
    private static final String AVG_CFE_PERCENTAGE_COL_NAME = "avgCfe";
    private static final int    AVG_CFE_PERCENTAGE_COL_INDEX = 2;

    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private final String influxUrl;
    private final String influxUser;
    private final String influxPassword;
    private final String influxOrg;
    private final String influxBucket;

    public SecondQueryDF(SparkSession spark,
                         String datasetPath,
                         String influxUrl,
                         String influxUser,
                         String influxPassword,
                         String influxOrg,
                         String influxBucket,
                         String resultsPath) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.influxUrl = influxUrl;
        this.influxUser = influxUser;
        this.influxPassword = influxPassword;
        this.influxOrg = influxOrg;
        this.influxBucket = influxBucket;
        this.resultsPath = resultsPath;
    }

    @Override
    public void close() {
        spark.stop();
    }

    @Override
    public void submit() {
        var averages = spark.read().parquet(datasetPath + ".parquet")
                .withColumn(YEAR_MONTH_COL_NAME, getYearMonth(CsvField.DATETIME_UTC.getName()))
                .select(col(YEAR_MONTH_COL_NAME),
                        col(CsvField.CARBON_INTENSITY_DIRECT.getName()),
                        col(CsvField.CFE_PERCENTAGE.getName()))
                .where(col(CsvField.COUNTRY.getName()).equalTo("Italy"))
                .groupBy(col(YEAR_MONTH_COL_NAME))
                .agg(avg(CsvField.CARBON_INTENSITY_DIRECT.getName()).as(AVG_CARBON_INTENSITY_COL_NAME),
                     avg(CsvField.CFE_PERCENTAGE.getName()).as(AVG_CFE_PERCENTAGE_COL_NAME))
                .orderBy(col(YEAR_MONTH_COL_NAME));
        var ciDesc = averages
                .orderBy(col(AVG_CARBON_INTENSITY_COL_NAME).desc())
                .limit(5);
        var ciAsc = averages
                .orderBy(col(AVG_CARBON_INTENSITY_COL_NAME).asc())
                .limit(5);
        var cfeDesc = averages
                .orderBy(col(AVG_CFE_PERCENTAGE_COL_NAME).desc())
                .limit(5);
        var cfeAsc = averages
                .orderBy(col(AVG_CFE_PERCENTAGE_COL_NAME).asc())
                .limit(5);
        saveToInfluxDB(averages);
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

    private void saveToInfluxDB(Dataset<Row> dataset) {
        dataset.foreachPartition(partition -> {
            var opts = InfluxDBClientOptions.builder()
                    .url(influxUrl)
                    .authenticate(influxUser, influxPassword.toCharArray())
                    .org(influxOrg)
                    .bucket(influxBucket)
                    .build();
            try (var client = InfluxDBClientFactory.create(opts);) {
                var writer = client.getWriteApiBlocking();
                var points = new ArrayList<Point>();
                partition.forEachRemaining(row -> {
                    var point = Point.measurement("carbon-intensity")
                            .addField("carbonIntensity", row.getDouble(AVG_CARBON_INTENSITY_COL_INDEX))
                            .addField("carbonFreeEnergyPercentage", row.getDouble(AVG_CFE_PERCENTAGE_COL_INDEX))
                            .time(getTime(row), WritePrecision.MS);
                    points.add(point);
                });
                writer.writePoints(points);
            }
        });
    }

    private Instant getTime(Row row) {
        return YearMonth.parse(row.getString(YEAR_MONTH_COL_INDEX))
                .atDay(1)
                .atStartOfDay()
                .toInstant(ZonedDateTime.now().getOffset());
    }
}
