package io.github.francescodonnini.query.q2;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.*;
import io.github.francescodonnini.query.q2.comparators.CarbonIntensityComparator;
import io.github.francescodonnini.query.q2.comparators.CfePercentageComparator;
import io.github.francescodonnini.query.q2.comparators.IntPairComparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class SecondQueryRDD extends AbstractQuery {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);
    private final String outputPath;
    private final InfluxDbWriterFactory factory;

    public SecondQueryRDD(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.outputPath = outputPath;
        this.factory = factory;
    }


    @Override
    public void submit() {
        var averages = getSparkSession().sparkContext().textFile(getInputPath(), 1)
                .toJavaRDD()
                .filter(this::italianZone)
                .mapToPair(this::getKVPair)
                .reduceByKey(Operators::sum3)
                .mapToPair(Operators::average3)
                .sortByKey(new IntPairComparator());
        var ciDesc = averages.takeOrdered(5, new CarbonIntensityComparator(false));
        var ciAsc = averages.takeOrdered(5, new CarbonIntensityComparator(true));
        var cfeDesc = averages.takeOrdered(5, new CfePercentageComparator(false));
        var cfeAsc = averages.takeOrdered(5, new CfePercentageComparator(true));
        if (shouldSave()) {
            save(averages, ciDesc, ciAsc, cfeDesc, cfeAsc);
        }
    }

    private boolean italianZone(String line) {
        var fields = getFields(line);
        return fields[CsvField.ZONE_ID.getIndex()].equals("IT");
    }

    private String[] getFields(String line) {
        return line.split(",");
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple3<Double, Double, Integer>> getKVPair(String line) {
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
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> averages,
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> ciDesc,
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> ciAsc,
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> cfeDesc,
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> cfeAsc) {
        save(averages);
        save(ciDesc, ciAsc, cfeDesc, cfeAsc);
    }

    private void save(
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> ciDesc,
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> ciAsc,
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> cfeDesc,
            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> cfeAsc) {
        try (var jsc = JavaSparkContext.fromSparkContext(getSparkSession().sparkContext())) {
            save(jsc.parallelizePairs(ciDesc, 1), "top-ci-desc.csv");
            save(jsc.parallelizePairs(ciAsc, 1), "top-ci-asc.csv");
            save(jsc.parallelizePairs(cfeDesc, 1), "top-cfe-desc.csv");
            save(jsc.parallelizePairs(cfeAsc, 1), "top-cfe-asc.csv");
        }
    }

    private void save(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> top, String fileName) {
        top.map(this::toCsv).saveAsTextFile(outputPath + "/" + fileName);
    }


    private void save(JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> result) {
        var csv = result.map(this::toCsv);
        csv.saveAsTextFile(outputPath + "/" + "plot.csv");
        result.foreachPartition(partition -> InfluxDbUtils.save(factory, partition, this::from));
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

    private String toCsv(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> x) {
        return x._1()._1() + "-" + x._1()._2() + "," + x._2()._1() + "," + x._2()._2();
    }
}
