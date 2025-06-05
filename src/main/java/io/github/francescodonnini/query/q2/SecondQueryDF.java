package io.github.francescodonnini.query.q2;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.ParquetField;
import io.github.francescodonnini.query.*;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;

import static org.apache.spark.sql.functions.*;

public class SecondQueryDF extends AbstractQuery {
    private static final String YEAR_MONTH_COL_NAME = "yearMonth";
    private static final int    YEAR_MONTH_COL_INDEX = 0;
    private static final String AVG_CARBON_INTENSITY_COL_NAME = "avgCarbonIntensity";
    private static final int    AVG_CARBON_INTENSITY_COL_INDEX = 1;
    private static final String AVG_CFE_PERCENTAGE_COL_NAME = "avgCfePercentage";
    private static final int    AVG_CFE_PERCENTAGE_COL_INDEX = 2;

    private final String resultsPath;
    private final InfluxDbWriterFactory factory;

    public SecondQueryDF(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.resultsPath = outputPath;
        this.factory = factory;
    }


    @Override
    public void submit() {
        var averages = getSparkSession().read().parquet(getInputPath())
                .withColumn(YEAR_MONTH_COL_NAME, getYearMonth(ParquetField.DATETIME_UTC.getName()))
                .select(col(YEAR_MONTH_COL_NAME),
                        col(ParquetField.CARBON_INTENSITY_DIRECT.getName()),
                        col(ParquetField.CFE_PERCENTAGE.getName()))
                .where(col(ParquetField.ZONE_ID.getName()).equalTo("IT"))
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
        var sortedPairs = ciDesc.unionByName(ciAsc)
                .unionByName(cfeDesc)
                .unionByName(cfeAsc);
        if (shouldSave()) {
            save(averages, sortedPairs);
        } else {
            collect(averages, sortedPairs);
        }

    }

    private Column getYearMonth(String colName) {
        return date_format(to_timestamp(col(colName), ParquetField.DATETIME_FORMAT), "yyyy-MM");
    }

    private void save(Dataset<Row> averages, Dataset<Row> sortedPairs) {
        saveToInfluxDB(averages);
        averages.write()
                .option("header", true)
                .csv(resultsPath + "-plots.csv");
        sortedPairs
                .write()
                .option("header", true)
                .csv(resultsPath + "-pairs.csv");
    }

    private void saveToInfluxDB(Dataset<Row> dataset) {
        dataset.foreachPartition((ForeachPartitionFunction<Row>) partition -> InfluxDbUtils.save(factory, partition, this::from));
    }

    private Point from(Row row) {
        return Point.measurement("result")
                .addField("carbonIntensity", row.getDouble(AVG_CARBON_INTENSITY_COL_INDEX))
                .addField("carbonFreeEnergyPercentage", row.getDouble(AVG_CFE_PERCENTAGE_COL_INDEX))
                .addTag("app", getAppName())
                .time(getTime(row), WritePrecision.MS);
    }

    private Instant getTime(Row row) {
        return TimeUtils.fromYearAndMonth(row.getString(YEAR_MONTH_COL_INDEX));
    }

    private void collect(Dataset<Row> averages, Dataset<Row> sortedPairs) {
        var avgList = averages.collectAsList();
        var pairList = sortedPairs.collectAsList();
        getSparkSession().logWarning(() -> String.format("#averageList = %d, #sortedPairs = %d%n", avgList.size(), pairList.size()));
    }
}
