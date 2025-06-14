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
    private static final int YEAR_MONTH_COL_INDEX = 0;
    private static final int AVG_CARBON_INTENSITY_COL_INDEX = 2;
    private static final int AVG_CFE_PERCENTAGE_COL_INDEX = 3;
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
                .withColumn(CommonOutputSchema.YEAR_MONTH, getYearMonth(ParquetField.DATETIME_UTC.getName()));
        final var tableName = "energyData";
        dataFrame.createOrReplaceTempView(tableName);
        var result = dataFrame.sqlContext()
                .sql(getSqlQuery());
        final var aggregatedTable = "aggregated";
        result.createOrReplaceTempView(aggregatedTable);
        var ciDesc = result.sqlContext()
                .sql(getTopBy(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT, false));
        var ciAsc = result.sqlContext()
                .sql(getTopBy(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT, true));
        var cfeDesc = result.sqlContext()
                .sql(getTopBy(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE, false));
        var cfeAsc = result.sqlContext()
                .sql(getTopBy(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE, true));
        if (shouldSave()) {
            save(result, ciDesc, ciAsc, cfeDesc, cfeAsc);
        } else {
            collect(result, ciDesc, ciAsc, cfeDesc, cfeAsc);
        }
    }

    private Column getYearMonth(String colName) {
        return date_format(to_timestamp(col(colName), ParquetField.DATETIME_FORMAT), "yyyy-MM");
    }

    private String getSqlQuery() {
        return "SELECT "
                + selectExpression() + "\n"
                + "FROM " + "energyData" + "\n"
                + "WHERE " + italianZone() + "\n"
                + "GROUP BY " + groupByExpression() + "\n"
                + "ORDER BY " + CommonOutputSchema.YEAR_MONTH + "\n";
    }

    private String selectExpression() {
        return String.join(",",
                CommonOutputSchema.YEAR_MONTH,
                ParquetField.ZONE_ID.getName(),
                avg(ParquetField.CARBON_INTENSITY_DIRECT, CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT),
                avg(ParquetField.CFE_PERCENTAGE, CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE));
    }

    private static String avg(ParquetField col, String alias) {
        return column(String.format("AVG(%s)", col.getName()), alias);
    }

    private static String column(String col, String alias) {
        return col + " AS " + alias;
    }

    private static String italianZone() {
        return String.format("%s = '%s'", ParquetField.ZONE_ID.getName(), "IT");
    }

    private static String groupByExpression() {
        return String.join(",", CommonOutputSchema.YEAR_MONTH, ParquetField.ZONE_ID.getName());
    }

    private String getTopBy(String colName, boolean asc) {
        var direction = asc ? "ASC" : "DESC";
        return "SELECT * FROM " + "aggregated" + "\n"
                + "ORDER BY " + colName + " " + direction + "\n"
                + "LIMIT " + 5;
    }

    private void save(Dataset<Row> averages, Dataset<Row> ciDesc, Dataset<Row> ciAsc, Dataset<Row> cfeDesc, Dataset<Row> cfeAsc) {
        save(averages);
        save(ciDesc, "top-ci-desc.csv");
        save(ciAsc, "top-ci-asc.csv");
        save(cfeDesc, "top-cfe-desc.csv");
        save(cfeAsc, "top-cfe-asc.csv");
    }

    private void save(Dataset<Row> averages) {
        saveToInfluxDB(averages);
        averages.drop(ParquetField.ZONE_ID.getName())
                .write()
                .option("header", true)
                .csv(outputPath + "/" + "plot.csv");
    }

    private void save(Dataset<Row> top, String fileName) {
        top.write()
           .option("header", true)
           .csv(outputPath + "/" + fileName);
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

    private void collect(Dataset<Row> averages, Dataset<Row> ciDesc, Dataset<Row> ciAsc, Dataset<Row> cfeDesc, Dataset<Row> cfeAsc) {
        var avgList = averages.collectAsList();
        var pairList = ciDesc.unionByName(ciAsc)
                .unionByName(cfeDesc)
                .unionByName(cfeAsc)
                .collectAsList();
        getSparkSession().logWarning(() -> String.format("#averageList = %d, #sortedPairs = %d%n", avgList.size(), pairList.size()));
    }
}
