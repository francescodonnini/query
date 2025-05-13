package io.github.francescodonnini.query;

import io.github.francescodonnini.dataset.CsvFields;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FirstQuery implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvFields.DATETIME_FORMAT);

    public FirstQuery(SparkSession spark, String datasetPath, String resultsPath) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
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
                .textFile(datasetPath, 1)
                .toJavaRDD();
        var averages = rdd.mapToPair(this::getPairsWithOccurrences)
                        .reduceByKey(QueryUtils::sumDoubleIntPair)
                        .mapToPair(QueryUtils::average);
        var pairs = rdd.mapToPair(this::getPairs);
        var max = pairs.reduceByKey(this::getMax);
        var min = pairs.reduceByKey(this::getMin);
        saveResult(averages, min, max);
    }

    private void saveResult(
            JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> averages,
            JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> min,
            JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Double>> max) {
        averages.join(min)
                .join(max)
                .saveAsTextFile(resultsPath);
    }

    private Tuple2<Tuple2<String, Integer>, Tuple2<Double, Double>> getPairs(String line) {
        var fields = line.split(",");
        return new Tuple2<>(getKey(fields), new Tuple2<>(getCID(fields), getCFE(fields)));
    }

    private Tuple2<Tuple2<String, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> getPairsWithOccurrences(String line) {
        var fields = line.split(",");
        return new Tuple2<>(getKey(fields), new Tuple2<>(new Tuple2<>(getCID(fields), 1), new Tuple2<>(getCFE(fields), 1)));
    }

    private Tuple2<Double, Double> getMax(Tuple2<Double, Double> x, Tuple2<Double, Double> y) {
        return new Tuple2<>(Math.max(x._1(), y._1()), Math.max(x._2(), y._2()));
    }

    private Tuple2<Double, Double> getMin(Tuple2<Double, Double> x, Tuple2<Double, Double> y) {
        return new Tuple2<>(Math.min(x._1(), y._1()), Math.min(x._2(), y._2()));
    }

    private double getCFE(String[] fields) {
        return Double.parseDouble(fields[CsvFields.CFE_PERCENTAGE]);
    }

    private double getCID(String[] fields) {
        return Double.parseDouble(fields[CsvFields.CARBON_INTENSITY_DIRECT]);
    }

    private Tuple2<String, Integer> getKey(String[] fields) {
        return new Tuple2<>(getCountry(fields), getYear(fields));
    }

    private String getCountry(String[] fields) {
        return fields[CsvFields.COUNTRY];
    }

    private Integer getYear(String[] fields) {
        return LocalDateTime.parse(fields[CsvFields.DATETIME_UTC], formatter).getYear();
    }
}
