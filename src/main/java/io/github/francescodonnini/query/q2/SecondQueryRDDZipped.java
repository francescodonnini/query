package io.github.francescodonnini.query.q2;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.*;
import io.github.francescodonnini.query.q2.comparators.FirstFieldComparator;
import io.github.francescodonnini.query.q2.comparators.IntPairComparator;
import io.github.francescodonnini.query.q2.comparators.SecondFieldComparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class SecondQueryRDDZipped extends AbstractQuery {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);
    private final String outputPath;
    private final InfluxDbWriterFactory factory;

    public SecondQueryRDDZipped(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.outputPath = outputPath;
        this.factory = factory;
    }


    @Override
    public void submit() {
        var averages = getSparkSession().sparkContext().textFile(getInputPath(), 1)
                .toJavaRDD()
                .filter(this::italianZone)
                .mapToPair(this::toPair)
                .reduceByKey(Operators::sum3)
                .mapToPair(Operators::average3);
        var swappedKeyValuePairs = averages
                .mapToPair(Tuple2::swap);
        var ciAsc = swappedKeyValuePairs
                .sortByKey(new FirstFieldComparator(true))
                .zipWithIndex()
                .filter(x -> x._2() < 5);
        var ciDesc = swappedKeyValuePairs
                .sortByKey(new FirstFieldComparator(false))
                .zipWithIndex()
                .filter(x -> x._2() < 5);
        var cfeAsc = swappedKeyValuePairs
                .sortByKey(new SecondFieldComparator(true))
                .zipWithIndex()
                .filter(x -> x._2() < 5);
        var cfeDesc = swappedKeyValuePairs
                .sortByKey(new SecondFieldComparator(false))
                .zipWithIndex()
                .filter(x -> x._2() < 5);
        if (shouldSave()) {
            save(averages.sortByKey(new IntPairComparator()), ciAsc, ciDesc, cfeAsc, cfeDesc);
        } else {
            collect(averages, ciAsc, ciDesc, cfeAsc, cfeDesc);
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

    private void save(
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> avg,
            JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> ciAsc,
            JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> ciDesc,
            JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> cfeAsc,
            JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> cfeDesc) {
        save(avg);
        save(ciAsc, ciDesc, "ci");
        save(cfeAsc, cfeDesc, "cfe");
    }

    private void save(
            JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> asc,
            JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> desc,
            String fileName) {
        save(asc.union(desc), fileName);
    }

    private void save(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> result) {
        result.map(this::toCsv).saveAsTextFile(outputPath + "/plots.csv");
        result.foreachPartition(partition -> {
            try (var client = factory.create()) {
                var writer = client.getWriteApiBlocking();
                var points = new ArrayList<Point>();
                partition.forEachRemaining(row -> points.add(from(row)));
                writer.writePoints(points);
            }
        });
    }

    private String toCsv(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x) {
        return x._1()._1() + "-" + x._1()._2() + "," + x._2()._1() + "," + x._2()._2();
    }

    private Point from(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> row) {
        var key = row._1();
        var val = row._2();
        return Point.measurement("result")
                .addField(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT_SHORT, val._1())
                .addField(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, val._2())
                .addTag("app", getAppName())
                .time(getTime(key), WritePrecision.MS);
    }

    private Instant getTime(Tuple2<Integer, Integer> key) {
        return TimeUtils.fromYearAndMonth(key._1(), key._2());
    }

    private void save(JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> pairs, String fileName) {
        pairs.map(this::toCsv2).saveAsTextFile(outputPath + "/" + fileName + ".csv");
    }

    private String toCsv2(Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> r) {
        var data = r._1();
        return data._2()._1() + "-" + data._2()._2() + "," + data._1()._1() + "," + data._1()._2();
    }

    private void collect(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> averages, JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> ciAsc, JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> ciDesc, JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> cfeAsc, JavaPairRDD<Tuple2<Tuple2<Double, Double>, Tuple2<Integer, Integer>>, Long> cfeDesc) {
        var c1 = ciAsc.count();
        var c2 = ciDesc.count();
        var c3 = cfeAsc.count();
        var c4 = cfeDesc.count();
        getSparkSession().logWarning(() -> String.format("averages count=%d, c1=%d, c2=%d, c3=%d, c4=%d%n", averages.count(), c1, c2, c3, c4));
    }
}
