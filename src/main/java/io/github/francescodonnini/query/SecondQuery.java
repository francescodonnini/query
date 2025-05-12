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
            .reduceByKey(this::sumPair)
            .map(this::getAvg)
            .saveAsTextFile(resultsPath);
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> getAvg(
            Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> x) {
        return new Tuple2<>(x._1(), new Tuple2<>(calculateAvg(x._2()._1()), calculateAvg(x._2()._1())));
    }

    private double calculateAvg(Tuple2<Double, Integer> x) {
        return x._1() / x._2();
    }

    private Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> sumPair(
            Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> x,
            Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> y) {
        var ciPair1 = x._1();
        var cfePair1 = x._2();
        var ciPair2 = y._1();
        var cfePair2 = y._2();
        return new Tuple2<>(sum2D(ciPair1, ciPair2), sum2D(cfePair1, cfePair2));
    }

    private Tuple2<Double, Integer> sum2D(Tuple2<Double, Integer> x, Tuple2<Double, Integer> y) {
        return new Tuple2<>(x._1() + y._1(), x._2() + y._2());
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
