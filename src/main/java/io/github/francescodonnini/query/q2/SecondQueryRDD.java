package io.github.francescodonnini.query.q2;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SecondQueryRDD implements Query {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private final InfluxDbWriterFactory factory;
    private final boolean save;

    public SecondQueryRDD(SparkSession spark,
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
        var averages = spark.sparkContext().textFile(datasetPath, 1)
                .toJavaRDD()
                .filter(this::italianZone)
                .mapToPair(this::toPair)
                .reduceByKey(Operators::sum3)
                .mapToPair(Operators::average3)
                .sortByKey(new IntPairComparator());
        var tops = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>();
        tops.addAll(averages.takeOrdered(5, new CarbonIntensityComparator(false)));
        tops.addAll(averages.takeOrdered(5, new CarbonIntensityComparator(true)));
        tops.addAll(averages.takeOrdered(5, new CfePercentageComparator(false)));
        tops.addAll(averages.takeOrdered(5, new CfePercentageComparator(true)));
        if (save) {
            save(averages);
            save(tops);
        } else {
            var s = String.format("averages count=%d%n", tops.size());
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
        var csv = result.map(this::toCsv);
        csv.saveAsTextFile(resultsPath + "-plots.csv");
        result.foreachPartition(partition -> InfluxDbUtils.save(factory, partition, this::from));
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

    private void save(List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> tops) {
        try (var jsc = new JavaSparkContext(spark.sparkContext())) {
            var csv = jsc.parallelize(tops)
                    .map(this::toCsv);
            csv.saveAsTextFile(resultsPath + "-pairs.csv");
        }
    }

    private String toCsv(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x) {
        return x._1()._1() + "-" + x._1()._2() + "," + x._2()._1() + "," + x._2()._2();
    }
}
