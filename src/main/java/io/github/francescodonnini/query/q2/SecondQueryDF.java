package io.github.francescodonnini.query.q2;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.ParquetField;
import io.github.francescodonnini.query.InfluxDbWriterFactory;
import io.github.francescodonnini.query.Query;
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
    private final InfluxDbWriterFactory factory;

    public SecondQueryDF(SparkSession spark, String datasetPath, String resultsPath, InfluxDbWriterFactory factory) {
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
        var averages = spark.read().parquet(datasetPath + ".parquet")
                .withColumn(YEAR_MONTH_COL_NAME, getYearMonth(ParquetField.DATETIME_UTC.getName()))
                .select(col(YEAR_MONTH_COL_NAME),
                        col(ParquetField.CARBON_INTENSITY_DIRECT.getName()),
                        col(ParquetField.CFE_PERCENTAGE.getName()))
                .where(col(ParquetField.COUNTRY.getName()).equalTo("Italy"))
                .groupBy(col(YEAR_MONTH_COL_NAME))
                .agg(avg(ParquetField.CARBON_INTENSITY_DIRECT.getName()).as(AVG_CARBON_INTENSITY_COL_NAME),
                     avg(ParquetField.CFE_PERCENTAGE.getName()).as(AVG_CFE_PERCENTAGE_COL_NAME))
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
        averages.write()
                .option("header", true)
                .csv(resultsPath + "-plots");
        ciDesc.unionByName(ciAsc)
              .unionByName(cfeDesc)
              .unionByName(cfeAsc)
              .write()
              .option("header", true)
              .csv(resultsPath + "-pairs");
    }

    private Column getYearMonth(String colName) {
        return date_format(to_timestamp(col(colName), ParquetField.DATETIME_FORMAT), "yyyy-MM");
    }

    private void saveToInfluxDB(Dataset<Row> dataset) {
        dataset.foreachPartition(partition -> {
            try (var client = factory.create()) {
                var writer = client.getWriteApiBlocking();
                var points = new ArrayList<Point>();
                partition.forEachRemaining(row -> {
                    var point = Point.measurement("q2-df")
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
