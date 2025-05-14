package io.github.francescodonnini.query;

import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.sql.SparkSession;

import java.time.format.DateTimeFormatter;

public class SecondQueryDF implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.getDateTimeFormat());

    public SecondQueryDF(SparkSession spark, String datasetPath, String resultsPath) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
    }

    @Override
    public void close() {
        spark.stop();
    }

    @Override
    public void submit() {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
