package io.github.francescodonnini.query;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SecondQueryDF implements Query {
    private static final String YEAR_MONTH_COL = "yearMonth";
    private static final String AVG_CARBON_INTENSITY_COL = "avgCi";
    private static final String AVG_CFE_PERCENTAGE_COL = "avgCfe";
    private final SparkSession spark;
    private final String datasetPath;
    private final InfluxDBClient influxDBClient;
    private final String resultsPath;

    public SecondQueryDF(SparkSession spark, String datasetPath, InfluxDBClient influxDBClient, String resultsPath) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.influxDBClient = influxDBClient;
        this.resultsPath = resultsPath;
    }

    @Override
    public void close() {
        spark.stop();
    }

    @Override
    public void submit() {
        var averages = spark.read().parquet(datasetPath + ".parquet")
                .withColumn(YEAR_MONTH_COL, getYearMonth(CsvField.DATETIME_UTC.getName()))
                .select(col(YEAR_MONTH_COL),
                        col(CsvField.COUNTRY.getName()),
                        col(CsvField.CARBON_INTENSITY_DIRECT.getName()),
                        col(CsvField.CFE_PERCENTAGE.getName()))
                .where(col(CsvField.COUNTRY.getName()).equalTo("Italy"))
                .groupBy(col(YEAR_MONTH_COL))
                .agg(avg(CsvField.CARBON_INTENSITY_DIRECT.getName()).as(AVG_CARBON_INTENSITY_COL),
                     avg(CsvField.CFE_PERCENTAGE.getName()).as(AVG_CFE_PERCENTAGE_COL))
                .orderBy(col(YEAR_MONTH_COL));
        var ciDesc = averages
                .orderBy(col(AVG_CARBON_INTENSITY_COL).desc())
                .limit(5);
        var ciAsc = averages
                .orderBy(col(AVG_CARBON_INTENSITY_COL).asc())
                .limit(5);
        var cfeDesc = averages
                .orderBy(col(AVG_CFE_PERCENTAGE_COL).desc())
                .limit(5);
        var cfeAsc = averages
                .orderBy(col(AVG_CFE_PERCENTAGE_COL).asc())
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

    private void saveToInfluxDB(Dataset<Row> dataset) {
        var writer = influxDBClient.getWriteApiBlocking();
        dataset.foreachPartition(rows -> {
            rows.forEachRemaining(row -> {
                var point = Point.measurement("ci")
                        .addTag("country", row.getAs(CsvField.COUNTRY.getName()))
                        .addTag("time", row.getAs(YEAR_MONTH_COL))
                        .addField("value", row.getDouble(AVG_CARBON_INTENSITY_COL));
            });
        });
    }
}
