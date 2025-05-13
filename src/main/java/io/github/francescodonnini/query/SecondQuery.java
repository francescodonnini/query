package io.github.francescodonnini.query;

import io.github.francescodonnini.dataset.CsvFields;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SecondQuery implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvFields.DATETIME_FORMAT);

    public SecondQuery(SparkSession spark, String datasetPath, String resultsPath) {
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
        spark.sparkContext()
            .textFile(datasetPath, 1)
            .toJavaRDD()
            .filter(line -> countryFilter(line, "IT"))
            .mapToPair(this::toPair)
            .reduceByKey(QueryUtils::sumDoubleIntPair)
            .map(QueryUtils::average)
            .saveAsTextFile(resultsPath);
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> toPair(String line) {
        var fields = getFields(line);
        var date = LocalDateTime.parse(fields[CsvFields.DATETIME_UTC], formatter);
        return new Tuple2<>(
                new Tuple2<>(date.getYear(), date.getMonth().getValue()),
                new Tuple2<>(
                        new Tuple2<>(Double.parseDouble(fields[CsvFields.CARBON_INTENSITY_DIRECT]), 1),
                        new Tuple2<>(Double.parseDouble(fields[CsvFields.CFE_PERCENTAGE]), 1)));
    }

    private boolean countryFilter(String line, String countryCode) {
        var fields = getFields(line);
        return fields[CsvFields.ZONE_ID].equals(countryCode);
    }

    private String[] getFields(String line) {
        return line.split(",");
    }
}
