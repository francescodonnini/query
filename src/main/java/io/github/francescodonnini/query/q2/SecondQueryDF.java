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
    private static final int YEAR_MONTH_COL_INDEX = 0;
    private static final int AVG_CARBON_INTENSITY_COL_INDEX = 1;
    private static final int AVG_CFE_PERCENTAGE_COL_INDEX = 2;

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
                .withColumn(CommonOutputSchema.YEAR_MONTH, getYearMonth())
                .select(col(CommonOutputSchema.YEAR_MONTH),
                        col(ParquetField.ZONE_ID.getName()),
                        col(ParquetField.CARBON_INTENSITY_DIRECT.getName()).as(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT),
                        col(ParquetField.CFE_PERCENTAGE.getName())).as(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE)
                .where(col(ParquetField.ZONE_ID.getName()).equalTo("IT"))
                .groupBy(col(CommonOutputSchema.YEAR_MONTH))
                .agg(avg(ParquetField.CARBON_INTENSITY_DIRECT.getName()).alias(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT),
                     avg(ParquetField.CFE_PERCENTAGE.getName()).alias(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE))
                .orderBy(col(CommonOutputSchema.YEAR_MONTH));
        var ciDesc = averages
                .orderBy(col(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT).desc())
                .limit(5);
        var ciAsc = averages
                .orderBy(col(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT).asc())
                .limit(5);
        var cfeDesc = averages
                .orderBy(col(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE).desc())
                .limit(5);
        var cfeAsc = averages
                .orderBy(col(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE).asc())
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

    private Column getYearMonth() {
        return date_format(to_timestamp(col(ParquetField.DATETIME_UTC.getName()), ParquetField.DATETIME_FORMAT), "yyyy-MM");
    }

    private void save(Dataset<Row> averages, Dataset<Row> sortedPairs) {
        saveToInfluxDB(averages);
        averages.drop(ParquetField.ZONE_ID.getName())
                .write()
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
                .addField(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT_SHORT, row.getDouble(AVG_CARBON_INTENSITY_COL_INDEX))
                .addField(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, row.getDouble(AVG_CFE_PERCENTAGE_COL_INDEX))
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
