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
    private static final int COUNTRY_COL_INDEX = 0;
    private static final int YEAR_COL_INDEX = 1;
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
                .withColumn(CommonOutputSchema.YEAR, year(to_timestamp(col(ParquetField.DATETIME_UTC.getName()), ParquetField.DATETIME_FORMAT)));
        final var result = executeQuery(dataFrame);
        if (shouldSave()) {
            save(result);
        } else {
            collect(result);
        }
    }

    private Dataset<Row> executeQuery(Dataset<Row> dataFrame) {
        final var tableName = "energyData";
        dataFrame.createOrReplaceTempView(tableName);
        String query = "SELECT " +
                selectExpression(
                        countryCol(),
                        CommonOutputSchema.YEAR,
                        aggCI(),
                        aggCFE()) + "\n" +
                "FROM " + tableName + "\n" +
                "GROUP BY " + groupByExpression(CommonOutputSchema.YEAR, CommonOutputSchema.COUNTRY);
        return dataFrame.sqlContext().sql(query);
    }

    private static String selectExpression(String... columns) {
        return String.join(", ", columns);
    }

    private static String countryCol() {
        return ParquetField.ZONE_ID.getName() + " AS " + CommonOutputSchema.COUNTRY;
    }

    private static String aggCI() {
        return String.format("AVG(%s) AS %s, ", ParquetField.CARBON_INTENSITY_DIRECT.getName(), CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT) +
                String.format("MIN(%s) AS %s, ", ParquetField.CARBON_INTENSITY_DIRECT.getName(), CommonOutputSchema.MIN_CARBON_INTENSITY_DIRECT) +
                String.format("MAX(%s) AS %s", ParquetField.CARBON_INTENSITY_DIRECT.getName(), CommonOutputSchema.MAX_CARBON_INTENSITY_DIRECT);
    }

    private static String aggCFE() {
        return String.format("AVG(%s) AS %s, ", ParquetField.CFE_PERCENTAGE.getName(), CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE) +
                String.format("MIN(%s) AS %s, ", ParquetField.CFE_PERCENTAGE.getName(), CommonOutputSchema.MIN_CARBON_FREE_ENERGY_PERCENTAGE) +
                String.format("MAX(%s) AS %s", ParquetField.CFE_PERCENTAGE.getName(), CommonOutputSchema.MAX_CARBON_FREE_ENERGY_PERCENTAGE);
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
