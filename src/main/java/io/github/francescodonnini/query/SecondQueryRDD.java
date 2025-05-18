package io.github.francescodonnini.query;

import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class SecondQueryRDD implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.getDateTimeFormat());

    public SecondQueryRDD(
            SparkSession spark,
            String datasetPath,
            String resultsPath) {
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
        var averages = spark.read()
                .parquet(datasetPath + ".parquet")
                .filter(this::italianZone)
                .javaRDD()
                .mapToPair(this::toPair)
                .reduceByKey(QueryUtils::sumDoubleIntPair)
                .mapToPair(QueryUtils::average)
                .sortByKey();

        var tops = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>();
        tops.addAll(averages.takeOrdered(5, (x, y) -> ciCompareTo(x, y, false)));
        tops.addAll(averages.takeOrdered(5, (x, y) -> ciCompareTo(x, y, true)));
        tops.addAll(averages.takeOrdered(5, (x, y) -> cfeCompareTo(x, y, false)));
        tops.addAll(averages.takeOrdered(5, (x, y) -> cfeCompareTo(x, y, true)));
    }

    private boolean italianZone(Row r) {
        return r.getString(CsvField.ZONE_ID.getIndex()).equals("IT");
    }

    private void saveToCache(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> plot) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private int cfeCompareTo(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> y, boolean asc) {
        return asc ? x._2()._2().compareTo(y._2()._2()) : y._2()._2().compareTo(x._2()._2());
    }

    private int ciCompareTo(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> y, boolean asc) {
        return asc ? x._2()._1().compareTo(y._2()._1()) : y._2()._1().compareTo(x._2()._1());
    }

    private String toCsv(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x) {
        return x._1()._1() + "," + x._1()._2() + "," + x._2()._1() + "," + x._2()._2();
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> toPair(Row line) {
        var date = LocalDateTime.parse(line.getString(CsvField.DATETIME_UTC.getIndex()), formatter);
        return new Tuple2<>(
                new Tuple2<>(date.getYear(), date.getMonth().getValue()),
                new Tuple2<>(
                        new Tuple2<>(line.getDouble(CsvField.CARBON_INTENSITY_DIRECT.getIndex()), 1),
                        new Tuple2<>(line.getDouble(CsvField.CFE_PERCENTAGE.getIndex()), 1)));
    }

}
