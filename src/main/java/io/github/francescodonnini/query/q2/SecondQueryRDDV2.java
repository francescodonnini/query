package io.github.francescodonnini.query.q2;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.InfluxDbWriterFactory;
import io.github.francescodonnini.query.Operators;
import io.github.francescodonnini.query.Query;
import io.github.francescodonnini.query.TimeUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;

public class SecondQueryRDDV2 implements Query {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private final InfluxDbWriterFactory factory;
    private final boolean save;

    public SecondQueryRDDV2(SparkSession spark,
                          String datasetPath,
                          String resultsPath,
                          InfluxDbWriterFactory factory,
                          boolean save) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
        this.factory = factory;
        this.save = save;
    }

    @Override
    public void close() {
        spark.stop();
    }

    @Override
    public void submit() {
        var averages = spark.sparkContext().textFile(datasetPath + ".csv", 1)
                .toJavaRDD()
                .filter(this::italianZone)
                .mapToPair(this::toPair)
                .reduceByKey(Operators::sum3)
                .mapToPair(Operators::average3);
        var count = averages.count();
        var swappedKeyValuePairs = averages.mapToPair(Tuple2::swap);
        var ci = swappedKeyValuePairs
                .sortByKey(Comparator.comparingDouble(Tuple2::_1))
                .zipWithIndex()
                .filter(x -> x._2() < 5 && x._2() >= count - 5);
        var cfe = swappedKeyValuePairs
                .sortByKey(Comparator.comparingDouble(Tuple2::_2))
                .zipWithIndex()
                .filter(x -> x._2() < 5 && x._2() >= count - 5);
        if (save) {
            save(averages.sortByKey(new IntPairComparator()));
            save(ci, cfe);
        } else {
            var ciList = ci.collect();
            var cfeList = cfe.collect();
            var s = String.format("averages count=%d, ci count=%d, cfe count=%d%n", count, ciList.size(), cfeList.size());
            spark.logWarning(() -> s);
        }
    }

    private boolean italianZone(String line) {
        var fields = getFields(line);
        return fields[CsvField.ZONE_ID.getIndex()].equals("IT");
    }

    private String[] getFields(String line) {
        return line.split(",");
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple3<Double, Double, Integer>> toPair(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getKey(fields), getValue(fields));
    }

    private Tuple2<Integer, Integer> getKey(String[] fields) {
        var date = LocalDateTime.parse(fields[CsvField.DATETIME_UTC.getIndex()], formatter);
        return new Tuple2<>(date.getYear(), date.getMonthValue());
    }

    private Tuple3<Double, Double, Integer> getValue(String[] fields) {
        return new Tuple3<>(Double.parseDouble(fields[CsvField.CARBON_INTENSITY_DIRECT.getIndex()]),
                Double.parseDouble(fields[CsvField.CFE_PERCENTAGE.getIndex()]),
                1);
    }

    private void save(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> result) {
        result.map(this::toCsv).saveAsTextFile(resultsPath + "-plots.csv");
        result.foreachPartition(partition -> {
            try (var client = factory.create()) {
                var writer = client.getWriteApiBlocking();
                var points = new ArrayList<Point>();
                partition.forEachRemaining(row -> points.add(from(row)));
                writer.writePoints(points);
            }
        });
    }

    private Point from(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> row) {
        var key = row._1();
        var val = row._2();
        return Point.measurement("result")
                .addField("avgCi", val._1())
                .addField("avgCfe", val._2())
                .addTag("app", spark.sparkContext().appName())
                .time(getTime(key), WritePrecision.MS);
    }

    private Instant getTime(Tuple2<Integer, Integer> key) {
        return TimeUtils.fromYearAndMonth(key._1(), key._2());
    }

    private String toCsv(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x) {
        return x._1()._1() + "-" + x._1()._2() + "," + x._2()._1() + "," + x._2()._2();
    }

    private void save(JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> ci, JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> cfe) {
        ci.map(this::toCsv2).saveAsTextFile(resultsPath + "ci-pairs.csv");
        cfe.map(this::toCsv2).saveAsTextFile(resultsPath + "cfe-pairs.csv");
    }

    private String toCsv2(Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> r) {
        var data = r._1();
        return data._2()._1() + "-" + data._2()._2() + "," + data._1()._1() + "," + data._1()._2();
    }
}
