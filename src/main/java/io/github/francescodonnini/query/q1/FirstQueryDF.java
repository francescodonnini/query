package io.github.francescodonnini.query.q1;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.ParquetField;
import io.github.francescodonnini.query.*;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;

import static org.apache.spark.sql.functions.*;

public class FirstQueryDF extends AbstractQuery {
    private static final int COUNTRY_COL_INDEX = 0;
    private static final int YEAR_COL_INDEX = 1;
    private final String outputPath;
    private final InfluxDbWriterFactory factory;

    public FirstQueryDF(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.outputPath = outputPath;
        this.factory = factory;
    }

    @Override
    public void submit() {
        var df = getSparkSession().read().parquet(getInputPath())
             .withColumn(CommonOutputSchema.YEAR, year(to_timestamp(col(ParquetField.DATETIME_UTC.getName()), ParquetField.DATETIME_FORMAT)))
             .select(col(CommonOutputSchema.YEAR),
                     col(ParquetField.ZONE_ID.getName()).as(CommonOutputSchema.COUNTRY),
                     col(ParquetField.CARBON_INTENSITY_DIRECT.getName()),
                     col(ParquetField.CFE_PERCENTAGE.getName()))
             .groupBy(col(CommonOutputSchema.COUNTRY),
                      col(CommonOutputSchema.YEAR))
             .agg(avg(ParquetField.CARBON_INTENSITY_DIRECT.getName()).alias(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT),
                  min(ParquetField.CARBON_INTENSITY_DIRECT.getName()).alias(CommonOutputSchema.MIN_CARBON_INTENSITY_DIRECT),
                  max(ParquetField.CARBON_INTENSITY_DIRECT.getName()).alias(CommonOutputSchema.MAX_CARBON_INTENSITY_DIRECT),
                  avg(ParquetField.CFE_PERCENTAGE.getName()).alias(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE) ,
                  min(ParquetField.CFE_PERCENTAGE.getName()).alias(CommonOutputSchema.MIN_CARBON_FREE_ENERGY_PERCENTAGE) ,
                  max(ParquetField.CFE_PERCENTAGE.getName()).alias(CommonOutputSchema.MAX_CARBON_FREE_ENERGY_PERCENTAGE));
        if (shouldSave()) {
            save(df);
        } else {
            collect(df);
        }
    }

    private void save(Dataset<Row> df) {
        df.orderBy(col(CommonOutputSchema.COUNTRY), col(CommonOutputSchema.YEAR))
          .write()
          .option("header", true)
          .csv(outputPath);
        df.foreachPartition((ForeachPartitionFunction<Row>) partition -> InfluxDbUtils.save(factory, partition, this::from));
    }

    private Point from(Row row) {
        return Point.measurement("result")
                .addField(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT_SHORT, row.getDouble(2))
                .addField(CommonOutputSchema.MIN_CARBON_INTENSITY_DIRECT_SHORT, row.getDouble(3))
                .addField(CommonOutputSchema.MAX_CARBON_INTENSITY_DIRECT_SHORT, row.getDouble(4))
                .addField(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, row.getDouble(5))
                .addField(CommonOutputSchema.MIN_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, row.getDouble(6))
                .addField(CommonOutputSchema.MAX_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, row.getDouble(7))
                .addTag(CommonOutputSchema.COUNTRY, row.getString(COUNTRY_COL_INDEX))
                .addTag("app", getAppName())
                .time(getTime(row), WritePrecision.MS);
    }

    private Instant getTime(Row row) {
        return TimeUtils.fromYear(row.getInt(YEAR_COL_INDEX));
    }

    private void collect(Dataset<Row> df) {
        var list = df.collectAsList();
        var s = String.format("total number of objects = %d%n", list.size());
        getSparkSession().logWarning(() -> s);
    }
}
