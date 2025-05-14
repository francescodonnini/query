package io.github.francescodonnini.query;

import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SecondQueryRDD implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.getDateTimeFormat());

    public SecondQueryRDD(SparkSession spark, String datasetPath, String resultsPath) {
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
            .filter(line -> countryFilter(line, "Italy"))
            .mapToPair(this::toPair)
            .reduceByKey(QueryUtils::sumDoubleIntPair)
            .map(QueryUtils::average)
            .map(this::stringify)
            .saveAsTextFile(resultsPath);
    }

    private String stringify(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x) {
        return x._1()._1() + "," + x._1()._2() + "," + x._2()._1() + "," + x._2()._2();
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> toPair(String line) {
        var fields = getFields(line);
        var date = LocalDateTime.parse(fields[CsvField.DATETIME_UTC.getIndex()], formatter);
        return new Tuple2<>(
                new Tuple2<>(date.getYear(), date.getMonth().getValue()),
                new Tuple2<>(
                        new Tuple2<>(Double.parseDouble(fields[CsvField.CARBON_INTENSITY_DIRECT.getIndex()]), 1),
                        new Tuple2<>(Double.parseDouble(fields[CsvField.CFE_PERCENTAGE.getIndex()]), 1)));
    }

    private boolean countryFilter(String line, String countryCode) {
        var fields = getFields(line);
        return fields[CsvField.COUNTRY.getIndex()].equals(countryCode);
    }

    private String[] getFields(String line) {
        return line.split(",");
    }
}
