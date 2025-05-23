package io.github.francescodonnini.query;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.dataset.CsvField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SecondQueryRDD implements Query {
    private static class IntPairComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
            var cmp = Integer.compare(o1._1(), o2._1());
            if (cmp == 0) {
                return Integer.compare(o1._2(), o2._2());
            }
            return cmp;
        }
    }
    private static class CarbonIntensityComparator implements Comparator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>, Serializable {
        private final boolean asc;

        public CarbonIntensityComparator(boolean asc) {
            this.asc = asc;
        }

        @Override
        public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
            return (asc ? 1 : -1) * Double.compare(o1._2()._1(), o2._2()._1());
        }
    }
    private static class CfePercentageComparator implements Comparator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>, Serializable {
        private final boolean asc;

        public CfePercentageComparator(boolean asc) {
            this.asc = asc;
        }

        @Override
        public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
            return (asc ? 1 : -1) * Double.compare(o1._2()._2(), o2._2()._2());
        }
    }

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private final InfluxDbWriterFactory factory;

    public SecondQueryRDD(SparkSession spark,
            String datasetPath,
            String resultsPath, InfluxDbWriterFactory factory) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
        this.factory = factory;
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
                .sortByKey(new IntPairComparator());
        save(averages);
        var tops = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>();
        tops.addAll(averages.takeOrdered(5, new CarbonIntensityComparator(false)));
        tops.addAll(averages.takeOrdered(5, new CarbonIntensityComparator(true)));
        tops.addAll(averages.takeOrdered(5, new CfePercentageComparator(false)));
        tops.addAll(averages.takeOrdered(5, new CfePercentageComparator(true)));
        save(tops);
    }

    private boolean italianZone(Row r) {
        return r.getString(CsvField.ZONE_ID.getIndex()).equals("IT");
    }

    private String toCsv(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x) {
        return x._1()._1() + "-" + x._1()._2() + "," + x._2()._1() + "," + x._2()._2();
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> toPair(Row line) {
        var date = LocalDateTime.parse(line.getString(CsvField.DATETIME_UTC.getIndex()), formatter);
        return new Tuple2<>(
                new Tuple2<>(date.getYear(), date.getMonth().getValue()),
                new Tuple2<>(
                        new Tuple2<>(line.getDouble(CsvField.CARBON_INTENSITY_DIRECT.getIndex()), 1),
                        new Tuple2<>(line.getDouble(CsvField.CFE_PERCENTAGE.getIndex()), 1)));
    }

    private void save(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> result) {
        result.map(this::toCsv).saveAsTextFile(resultsPath + "-plots.csv");
        result.foreachPartition(partition -> {
            try (var client = factory.create()) {
                var writer = client.getWriteApiBlocking();
                partition.forEachRemaining(row -> writer.writePoint(from(row)));
            }
        });
    }

    private Point from(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> row) {
        var key = row._1();
        var val = row._2();
        return Point.measurement("q2-rdd")
                .addField("avgCi", val._1())
                .addField("avgCfe", val._2())
                .time(getYearMonth(key), WritePrecision.S);
    }

    private Instant getYearMonth(Tuple2<Integer, Integer> key) {
        var year = key._1();
        var month = key._2();
        return YearMonth.of(year, month)
                .atDay(1)
                .atStartOfDay()
                .toInstant(ZonedDateTime.now().getOffset());
    }

    private void save(List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> tops) {
        try (var jsc = new JavaSparkContext(spark.sparkContext())) {
            jsc.parallelize(tops)
                    .map(this::toCsv)
                    .saveAsTextFile(resultsPath + "-pairs.csv");
        }
    }
}
