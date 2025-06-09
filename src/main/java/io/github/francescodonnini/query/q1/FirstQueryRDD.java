package io.github.francescodonnini.query.q1;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FirstQueryRDD extends AbstractQuery {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);
    private final String outputPath;
    private final InfluxDbWriterFactory factory;

    public FirstQueryRDD(SparkSession spark, String inputPath, boolean save, String outputPath, InfluxDbWriterFactory factory) {
        super(spark, inputPath, save);
        this.outputPath = outputPath;
        this.factory = factory;
    }

    @Override
    public void submit() {
        var rdd = getSparkSession().sparkContext()
                .textFile(getInputPath(), 1)
                .toJavaRDD()
                .mapToPair(this::getKVPair);
        var averages = rdd
                        .reduceByKey(Operators::sum3)
                        .mapToPair(Operators::average3);
        var max = rdd
                    .reduceByKey(this::getMax)
                    .mapToPair(this::getDoubles);
        var min = rdd
                    .reduceByKey(this::getMin)
                    .mapToPair(this::getDoubles);
        if (shouldSave()) {
            save(averages, min, max);
        } else {
            collect(averages, min, max);
        }
    }

    private Tuple2<Tuple2<String, Integer>, Tuple3<Double, Double, Integer>> getKVPair(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getKey(fields), new Tuple3<>(getCID(fields), getCFE(fields), 1));
    }

    private double getCFE(String[] fields) {
        return Double.parseDouble(fields[CsvField.CFE_PERCENTAGE.getIndex()]);
    }

    private double getCID(String[] fields) {
        return Double.parseDouble(fields[CsvField.CARBON_INTENSITY_DIRECT.getIndex()]);
    }

    private Tuple2<String, Integer> getKey(String[] fields) {
        return new Tuple2<>(getCountry(fields), getYear(fields));
    }

    private String getCountry(String[] fields) {
        return fields[CsvField.ZONE_ID.getIndex()];
    }

    private Integer getYear(String[] fields) {
        return LocalDateTime.parse(fields[CsvField.DATETIME_UTC.getIndex()], formatter).getYear();
    }

    private String[] getFields(String line) {
        return line.split(",");
    }

    private Tuple3<Double, Double, Integer> getMax(Tuple3<Double, Double, Integer> x, Tuple3<Double, Double, Integer> y) {
        return new Tuple3<>(Math.max(x._1(), y._1()), Math.max(x._2(), y._2()), 1);
    }

    private Tuple3<Double, Double, Integer> getMin(Tuple3<Double, Double, Integer> x, Tuple3<Double, Double, Integer> y) {
        return new Tuple3<>(Math.min(x._1(), y._1()), Math.min(x._2(), y._2()), 1);
    }

    private Tuple2<Tuple2<String, Integer>, Tuple2<Double, Double>> getDoubles(Tuple2<Tuple2<String, Integer>, Tuple3<Double, Double, Integer>> x) {
        return new Tuple2<>(x._1(), new Tuple2<>(x._2()._1(), x._2()._2()));
    }

    private void save(JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> averages, JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> min, JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> max) {
        save(averages.join(min).join(max));
    }

    private void save(JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Tuple2<Double, Double>>> result) {
        var csv = result
                .sortByKey()
                .map(this::toCsv);
        csv.saveAsTextFile(outputPath);
        result.foreachPartition(partition -> InfluxDbUtils.save(factory, partition, this::from));
    }

    private Point from(Tuple2<Tuple2<String, Integer>, Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Tuple2<Double, Double>>> row) {
        var key = row._1();
        var val = row._2();
        var avg = val._1()._1();
        var max = val._1()._2();
        var min = val._2();
        return Point.measurement("result")
                .addField(CommonOutputSchema.AVG_CARBON_INTENSITY_DIRECT_SHORT, avg._1())
                .addField(CommonOutputSchema.AVG_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, avg._2())
                .addField(CommonOutputSchema.MAX_CARBON_INTENSITY_DIRECT_SHORT, max._1())
                .addField(CommonOutputSchema.MAX_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, max._2())
                .addField(CommonOutputSchema.MIN_CARBON_INTENSITY_DIRECT_SHORT, min._1())
                .addField(CommonOutputSchema.MIN_CARBON_FREE_ENERGY_PERCENTAGE_SHORT, min._2())
                .addTag(CommonOutputSchema.COUNTRY, key._1())
                .addTag("app", getAppName())
                .time(TimeUtils.fromYear(key._2()), WritePrecision.MS);
    }

    private String toCsv(
            Tuple2<Tuple2<String, Integer>, Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Tuple2<Double, Double>>> x) {
        return key(x._1()) + "," + value(x._2());
    }

    private static String key(Tuple2<String, Integer> key) {
        return key._1() + "," + key._2();
    }

    private static String value(Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Tuple2<Double, Double>> val) {
        var avg = val._1()._1();
        var min = val._1()._2();
        var max = val._2();
        return carbonIntensity(avg, min, max) + "," + cfePercentage(avg, min, max);
    }

    private static String carbonIntensity(Tuple2<Double, Double> avg, Tuple2<Double, Double> min, Tuple2<Double, Double> max) {
        return avg._1() + "," + min._1() + "," + max._1();
    }

    private static String cfePercentage(Tuple2<Double, Double> avg, Tuple2<Double, Double> min, Tuple2<Double, Double> max) {
        return avg._2() + "," + min._2() + "," + max._2();
    }

    private void collect(JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> averages, JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> min, JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> max) {
        var list = averages
                .join(min)
                .join(max)
                .collect();
        var s = String.format("total number of objects = %d%n", list.size());
        getSparkSession().logWarning(() -> s);
    }
}
