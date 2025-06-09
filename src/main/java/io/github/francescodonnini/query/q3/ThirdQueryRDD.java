package io.github.francescodonnini.query.q3;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ThirdQueryRDD extends AbstractQuery {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);
    private final String outputPath;
    private final InfluxDbWriterFactory factory;

    public ThirdQueryRDD(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.outputPath = outputPath;
        this.factory = factory;
    }

    @Override
    public void submit() {
        var lines = getSparkSession().sparkContext()
                .textFile(getInputPath(), 1)
                .toJavaRDD();
        var averages = lines.mapToPair(this::getPairWithOnes)
                .reduceByKey(Operators::sum3)
                .map(Operators::average3);
        var cfeQuantiles = lines.mapToPair(this::getCfe)
                .groupByKey()
                .mapValues(this::toSortedList)
                .mapValues(this::getQuantiles);
        var ciQuantiles = lines.mapToPair(this::getCi)
                .groupByKey()
                .mapValues(this::toSortedList)
                .mapValues(this::getQuantiles);
        if (shouldSave()) {
            save(averages, ciQuantiles, cfeQuantiles);
        } else {
            collect(averages, cfeQuantiles, ciQuantiles);
        }
    }

    private void collect(JavaRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> averages, JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> cfeQuantiles, JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> ciQuantiles) {
        var list1 = averages.collect();
        var list2 = ciQuantiles.collect();
        var list3 = cfeQuantiles.collect();
        getSparkSession().logWarning(() -> String.format("#averages = %d, #ciQuantiles = %d, #cfeQuantiles=%d", list1.size(), list2.size(), list3.size()));
    }

    private Tuple2<Tuple2<Integer, Integer>, Tuple3<Double, Double, Integer>> getPairWithOnes(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getYearDayOfYearPair(fields), getTriplet(fields));
    }

    private Tuple2<Integer, Integer> getYearDayOfYearPair(String[] fields) {
        var date = LocalDateTime.parse(getDatetime(fields), formatter);
        return new Tuple2<>(date.getYear(), date.getDayOfYear());
    }

    private Tuple3<Double, Double, Integer> getTriplet(String[] fields) {
        return new Tuple3<>(getCi(fields), getCfe(fields), 1);
    }

    private Tuple3<Double, Double, Double> getQuantiles(List<Double> list) {
        var size = list.size();
        return new Tuple3<>(list.get((int) (size * .25)), list.get((int) (size * .5)), list.get((int) (size * .75)));
    }

    private List<Double> toSortedList(Iterable<Double> it) {
        var list = new ArrayList<Double>();
        it.forEach(list::add);
        list.sort(Double::compareTo);
        return list;
    }

    private Tuple2<Tuple2<String, Integer>, Double> getCfe(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getKey(fields), getCfe(fields));
    }

    private Tuple2<Tuple2<String, Integer>, Double> getCi(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getKey(fields), getCi(fields));
    }

    private Tuple2<String, Integer> getKey(String[] fields) {
        return new Tuple2<>(getCountry(fields), getHourOfDay(fields));
    }

    private double getCfe(String[] fields) {
        return Double.parseDouble(fields[CsvField.CFE_PERCENTAGE.getIndex()]);
    }

    private double getCi(String[] fields) {
        return Double.parseDouble(fields[CsvField.CARBON_INTENSITY_DIRECT.getIndex()]);
    }

    private int getHourOfDay(String[] fields) {
        return LocalDateTime.parse(getDatetime(fields), formatter).getHour();
    }

    private String getDatetime(String[] fields) {
        return fields[CsvField.DATETIME_UTC.getIndex()];
    }

    private String getCountry(String[] fields) {
        return fields[CsvField.ZONE_ID.getIndex()];
    }

    private String[] getFields(String line) {
        return line.split(",");
    }

    private void save(
            JavaRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> averages,
            JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> ciQuantiles,
            JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> cfeQuantiles) {
        save(averages);
        save(cfeQuantiles, "/quantiles/cfe.csv");
        save(ciQuantiles, "/quantiles/ci.csv");
    }

    private void save(JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> quantiles, String fileName) {
        quantiles.map(this::quantileToCsv)
                .saveAsTextFile(outputPath + fileName);
    }

    private String quantileToCsv(Tuple2<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> x) {
        return x._1()._1() + "," + x._1()._2() + "," + x._2()._1() + "," + x._2()._2() + "," + x._2()._3();
    }

    private void save(JavaRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> averages) {
        averages.map(this::toCsv)
                .saveAsTextFile(outputPath + "/averages.csv");
        averages.foreachPartition(partition -> InfluxDbUtils.save(factory, partition, this::from));
    }

    private String toCsv(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> avg) {
        var date = fromYearAndDayOfYear(avg._1()).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return date + "," + avg._2()._1() + "," + avg._2()._2();
    }

    private LocalDate fromYearAndDayOfYear(Tuple2<Integer, Integer> yearDayOfYear) {
        return LocalDate.ofYearDay(yearDayOfYear._1(), yearDayOfYear._2());
    }

    private Point from(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> avg) {
        return Point.measurement("result")
                .addField("avgCi", avg._2()._1())
                .addField("avgCfe", avg._2()._2())
                .addTag("app", getAppName())
                .time(getTime(avg._1()), WritePrecision.MS);
    }

    private Instant getTime(Tuple2<Integer, Integer> yearDayOfYear) {
        return TimeUtils.fromYearAndDayOfYear(yearDayOfYear._1(), yearDayOfYear._2());
    }
}
