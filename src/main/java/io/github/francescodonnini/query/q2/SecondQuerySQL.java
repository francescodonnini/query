package io.github.francescodonnini.query.q2;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.ParquetField;
import io.github.francescodonnini.query.AbstractQuery;
import io.github.francescodonnini.query.InfluxDbUtils;
import io.github.francescodonnini.query.InfluxDbWriterFactory;
import io.github.francescodonnini.query.TimeUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.to_timestamp;

public class SecondQuerySQL extends AbstractQuery {
    private static final String YEAR_MONTH_COL_NAME = "yearMonth";
    private static final int    YEAR_MONTH_COL_INDEX = 0;
    private static final String COUNTRY_COL_NAME = "country";
    private static final String AVG_CARBON_INTENSITY_COL_NAME = "avgCarbonIntensity";
    private static final int    AVG_CARBON_INTENSITY_COL_INDEX = 1;
    private static final String AVG_CFE_PERCENTAGE_COL_NAME = "avgCfePercentage";
    private static final int    AVG_CFE_PERCENTAGE_COL_INDEX = 2;
    private final String outputPath;
    private final InfluxDbWriterFactory factory;

    public SecondQuerySQL(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.outputPath = outputPath;
        this.factory = factory;
    }

    @Override
    public void submit() {
        var dataFrame = getSparkSession().read().parquet(getInputPath())
                .withColumn(YEAR_MONTH_COL_NAME, getYearMonth(ParquetField.DATETIME_UTC.getName()));
        final var tableName = "energyData";
        dataFrame.createOrReplaceTempView(tableName);
        var result = dataFrame.sqlContext()
                .sql(getSqlQuery(tableName));
        var ciDesc = result.sqlContext()
                .sql(getTopBy(tableName, AVG_CARBON_INTENSITY_COL_NAME, false, 5));
        var ciAsc = result.sqlContext()
                .sql(getTopBy(tableName, AVG_CARBON_INTENSITY_COL_NAME, true, 5));
        var cfeDesc = result.sqlContext()
                .sql(getTopBy(tableName, AVG_CFE_PERCENTAGE_COL_NAME, false, 5));
        var cfeAsc = result.sqlContext()
                .sql(getTopBy(tableName, AVG_CFE_PERCENTAGE_COL_NAME, true, 5));
        var sortedPairs = ciDesc.unionByName(ciAsc)
                .unionByName(cfeDesc)
                .unionByName(cfeAsc);
        if (shouldSave()) {
            save(result, sortedPairs);
        } else {
            collect(result, sortedPairs);
        }
    }

    private Column getYearMonth(String colName) {
        return date_format(to_timestamp(col(colName), ParquetField.DATETIME_FORMAT), "yyyy-MM");
    }

    private String getSqlQuery(String table) {
        return "SELECT "
                + selectExpression() + "\n"
                + "FROM " + table + "\n"
                + "WHERE " + eq(ParquetField.ZONE_ID, "IT") + "\n"
                + "GROUP BY " + groupByExpression() + "\n"
                + "ORDER BY " + YEAR_MONTH_COL_NAME + "\n";
    }

    private String selectExpression() {
        return String.join(",",
                YEAR_MONTH_COL_NAME,
                column(ParquetField.ZONE_ID, COUNTRY_COL_NAME),
                avg(ParquetField.CARBON_INTENSITY_DIRECT, AVG_CARBON_INTENSITY_COL_NAME),
                avg(ParquetField.CFE_PERCENTAGE, AVG_CFE_PERCENTAGE_COL_NAME));
    }

    private static String column(ParquetField col, String alias) {
        return column(col.getName(), alias);
    }

    private static String avg(ParquetField col, String alias) {
        return column(String.format("AVG(%s)", col.getName()), alias);
    }

    private static String column(String col, String alias) {
        return col + " AS " + alias;
    }

    private static String eq(ParquetField col, String value) {
        return String.format("%s = '%s'", col.getName(), value);
    }

    private static String groupByExpression() {
        return String.join(",", YEAR_MONTH_COL_NAME, COUNTRY_COL_NAME);
    }

    private String getTopBy(String table, String colName, boolean asc, int limit) {
        var direction = asc ? "ASC" : "DESC";
        return "SELECT * FROM " + table + "\n"
                + "ORDER BY " + colName + " " + direction + "\n"
                + "LIMIT " + limit;
    }

    private void save(Dataset<Row> averages, Dataset<Row> sortedPairs) {
        saveToInfluxDB(averages);
        averages.write()
                .option("header", true)
                .csv(outputPath + "-plots.csv");
        sortedPairs
                .write()
                .option("header", true)
                .csv(outputPath + "-pairs.csv");
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
