package io.github.francescodonnini.query.q1;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.InfluxDbWriterFactory;
import io.github.francescodonnini.query.Query;
import io.github.francescodonnini.query.Operators;
import io.github.francescodonnini.query.TimeUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FirstQueryRDD implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private final InfluxDbWriterFactory factory;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);


    public FirstQueryRDD(SparkSession spark, String datasetPath, String resultsPath, InfluxDbWriterFactory factory) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
        this.factory = factory;
    }

    @Override
    public void close() {
        spark.stop();
    }

    /**
     * Facendo riferimento al dataset dei valori energetici dell’Italia e della Svezia, aggregare i dati su base
     * annua. Calcolare la media, il minimo e il massimo di “Carbon intensity gCO2 eq/kWh (direct)” e
     * “Carbon-free energy percentage (CFE%)” per ciascun anno dal 2021 al 2024. Inoltre, considerando il
     * valor medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”
     * aggregati su base annua, generare due grafici che consentano di confrontare visivamente l’andamento
     * per Italia e Svezia.
     * csv format
     * Datetime (UTC),Country,Zone name,Zone id,Carbon intensity gCO₂eq/kWh (direct),Carbon intensity gCO₂eq/kWh (Life cycle),Carbon-free energy percentage (CFE%),Renewable energy percentage (RE%),Data source,Data estimated,Data estimation method
     * 2021-01-01 00:00:00,Sweden,Sweden,SE,2.54,30.02,99.42,60.13,svk.se,true,ESTIMATED_FORECASTS_HIERARCHY
     * 2021-01-01 01:00:00,Sweden,Sweden,SE,2.6,30.32,99.4,59.52,svk.se,true,ESTIMATED_FORECASTS_HIERARCHY
     */
    @Override
    public void submit() {
        var rdd = spark.sparkContext()
                .textFile(datasetPath + ".csv", 1)
                .toJavaRDD();
        var averages = rdd.mapToPair(this::getPairsWithOccurrences)
                        .reduceByKey(Operators::sumDoubleIntPair)
                        .mapToPair(Operators::average);
        var pairs = rdd.mapToPair(this::getPairs);
        var max = pairs.reduceByKey(this::getMax);
        var min = pairs.reduceByKey(this::getMin);
        saveResult(averages, min, max);
    }

    private Tuple2<Tuple2<String, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> getPairsWithOccurrences(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getKey(fields), new Tuple2<>(new Tuple2<>(getCID(fields), 1), new Tuple2<>(getCFE(fields), 1)));
    }

    private Tuple2<Tuple2<String, Integer>, Tuple2<Double, Double>> getPairs(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getKey(fields), new Tuple2<>(getCID(fields), getCFE(fields)));
    }

    private String[] getFields(String line) {
        return line.split(",");
    }

    private Tuple2<Double, Double> getMax(Tuple2<Double, Double> x, Tuple2<Double, Double> y) {
        return new Tuple2<>(Math.max(x._1(), y._1()), Math.max(x._2(), y._2()));
    }

    private Tuple2<Double, Double> getMin(Tuple2<Double, Double> x, Tuple2<Double, Double> y) {
        return new Tuple2<>(Math.min(x._1(), y._1()), Math.min(x._2(), y._2()));
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

    private void saveResult(
            JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> averages,
            JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> min,
            JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> max) {
        var result = averages.join(min)
                .join(max);
        result.map(this::toCsv).saveAsTextFile(resultsPath);
        result.foreachPartition(partition -> {
            try (var client = factory.create()) {
                var writer = client.getWriteApiBlocking();
                partition.forEachRemaining(row -> writer.writePoint(from(row)));
            }
        });
    }

    private Point from(Tuple2<Tuple2<String, Integer>, Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Tuple2<Double, Double>>> row) {
        var key = row._1();
        var val = row._2();
        var avg = val._1()._1();
        var max = val._1()._2();
        var min = val._2();
        return Point.measurement("q1-rdd")
                .addTag("country", key._1())
                .addField("avgCi", avg._1())
                .addField("avgCfe", avg._2())
                .addField("maxCi", max._1())
                .addField("maxCfe", max._2())
                .addField("minCi", min._1())
                .addField("minCfe", min._2())
                .time(TimeUtils.fromYear(key._2()), WritePrecision.S);
    }

    private String toCsv(
            Tuple2<Tuple2<String, Integer>, Tuple2<Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Tuple2<Double, Double>>> x) {
        return x._1()._1() + "," + x._1()._2() + "," + x._2()._1()._1()._1() + "," + x._2()._1()._1()._2() + "," + x._2()._2()._1() + "," + x._2()._2()._2();
    }
}
