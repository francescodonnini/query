package io.github.francescodonnini.query.q1;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.ParquetField;
import io.github.francescodonnini.query.AbstractQuery;
import io.github.francescodonnini.query.InfluxDbUtils;
import io.github.francescodonnini.query.InfluxDbWriterFactory;
import io.github.francescodonnini.query.TimeUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;

import static org.apache.spark.sql.functions.*;

public class FirstQuerySQL extends AbstractQuery {
    private static final String YEAR_COL_NAME = "year";
    private static final int YEAR_COL_INDEX = 0;
    private static final String COUNTRY_COL_NAME = "country";
    private static final int COUNTRY_COL_INDEX = 1;
    private final String outputPath;
    private final InfluxDbWriterFactory factory;

    public FirstQuerySQL(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.outputPath = outputPath;
        this.factory = factory;
    }

    @Override
    public void submit() {
        final var dataFrame = getSparkSession().read().parquet(getInputPath())
                .withColumn(YEAR_COL_NAME, year(to_timestamp(col(ParquetField.DATETIME_UTC.getName()), ParquetField.DATETIME_FORMAT)));
        final var result = executeQuery(dataFrame);
        if (shouldSave()) {
            save(result);
        } else {
            collect(result);
        }
    }

    private Dataset<Row> executeQuery(Dataset<Row> dataFrame) {
        final var carbonIntensityCol = "CarbonIntensity";
        final var cfePercentageCol = "CfePercentage";
        final var tableName = "energyData";
        dataFrame.createOrReplaceTempView(tableName);
        String query = "SELECT " +
                selectExpression(
                        column(ParquetField.ZONE_ID, COUNTRY_COL_NAME),
                        YEAR_COL_NAME,
                        agg(ParquetField.CARBON_INTENSITY_DIRECT, carbonIntensityCol),
                        agg(ParquetField.CFE_PERCENTAGE, cfePercentageCol)) + "\n" +
                "FROM " + tableName + "\n" +
                "GROUP BY " + groupByExpression(YEAR_COL_NAME, COUNTRY_COL_NAME);
        return dataFrame.sqlContext().sql(query);
    }

    private static String selectExpression(String... columns) {
        return String.join(", ", columns);
    }

    private static String column(ParquetField col, String alias) {
        return col.getName() + " AS " + alias;
    }

    private static String agg(ParquetField col, String alias) {
        return String.format("AVG(%s) AS avg%s, ", col.getName(), alias) +
                String.format("MIN(%s) AS min%s, ", col.getName(), alias) +
                String.format("MAX(%s) AS max%s", col.getName(), alias);
    }

    private static String groupByExpression(String... columns) {
        return String.join(", ", columns) + " ";
    }

    private void save(Dataset<Row> result) {
        result.write()
                .option("header", true)
                .csv(outputPath);
        result.foreachPartition((ForeachPartitionFunction<Row>) p -> InfluxDbUtils.save(factory, p, this::createPoint));
    }

    private Point createPoint(Row row) {
        return com.influxdb.client.write.Point.measurement("result")
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
