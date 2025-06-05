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
    private static final String COUNTRY_COL_NAME = "country";
    private static final int COUNTRY_COL_INDEX = 0;
    private static final String YEAR_COL_NAME = "year";
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
        final var carbonIntensityCol = "carbonIntensity";
        final var cfePercentageCol = "cfePercentage";
        var df = getSparkSession().read().parquet(getInputPath())
             .withColumn(YEAR_COL_NAME, year(to_timestamp(col(ParquetField.DATETIME_UTC.getName()), ParquetField.DATETIME_FORMAT)))
             .select(col(YEAR_COL_NAME),
                     col(ParquetField.ZONE_ID.getName()).as(COUNTRY_COL_NAME),
                     col(ParquetField.CARBON_INTENSITY_DIRECT.getName()).as(carbonIntensityCol),
                     col(ParquetField.CFE_PERCENTAGE.getName()).as(cfePercentageCol))
             .groupBy(col(COUNTRY_COL_NAME),
                      col(YEAR_COL_NAME))
             .agg(avg(col(carbonIntensityCol)),
                  min(col(carbonIntensityCol)),
                  max(col(carbonIntensityCol)),
                  avg(col(cfePercentageCol)),
                  max(col(cfePercentageCol)),
                  min(col(cfePercentageCol)));
        if (shouldSave()) {
            save(df);
        } else {
            collect(df);
        }
    }

    private void save(Dataset<Row> df) {
        df.write()
          .option("header", true)
          .csv(outputPath + ".csv");
        df.foreachPartition((ForeachPartitionFunction<Row>) partition -> InfluxDbUtils.save(factory, partition, this::from));
    }


    private Point from(Row row) {
        return Point.measurement("result")
                .addTag(COUNTRY_COL_NAME, row.getString(COUNTRY_COL_INDEX))
                .addField("avgCi", row.getDouble(2))
                .addField("minCi", row.getDouble(3))
                .addField("maxCi", row.getDouble(4))
                .addField("avgCfe", row.getDouble(5))
                .addField("minCfe", row.getDouble(6))
                .addField("maxCfe", row.getDouble(7))
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
