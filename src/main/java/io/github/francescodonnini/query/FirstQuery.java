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
     *
     * csv format
     * Datetime (UTC),Country,Zone name,Zone id,Carbon intensity gCO₂eq/kWh (direct),Carbon intensity gCO₂eq/kWh (Life cycle),Carbon-free energy percentage (CFE%),Renewable energy percentage (RE%),Data source,Data estimated,Data estimation method
     * 2021-01-01 00:00:00,Sweden,Sweden,SE,2.54,30.02,99.42,60.13,svk.se,true,ESTIMATED_FORECASTS_HIERARCHY
     * 2021-01-01 01:00:00,Sweden,Sweden,SE,2.6,30.32,99.4,59.52,svk.se,true,ESTIMATED_FORECASTS_HIERARCHY
     */
    @Override
    public void submit() {
        var lines = spark.sparkContext()
                .textFile(datasetPath, 1)
                .toJavaRDD();
        var carbonIntensity = lines.mapToPair(this::getCIDPair);
        var avgCarbonIntensity = calculateAvg(carbonIntensity);
        var maxCarbonIntensity = calculateMax(carbonIntensity);
        var minCarbonIntensity = calculateMin(carbonIntensity);
        var cfe = lines.mapToPair(this::getCFEPair);
        var avgCfe = calculateAvg(cfe);
        var maxCfe = calculateMax(cfe);
        var minCfe = calculateMin(cfe);
        saveResult(avgCarbonIntensity, minCarbonIntensity, maxCarbonIntensity, avgCfe, minCfe, maxCfe);
    }

    private void saveResult(
            JavaPairRDD<Integer, Double> avgCI,
            JavaPairRDD<Integer, Double> minCI,
            JavaPairRDD<Integer, Double> maxCI,
            JavaPairRDD<Integer, Double> avgCfe,
            JavaPairRDD<Integer, Double> minCfe,
            JavaPairRDD<Integer, Double> maxCfe) {
        avgCI.union(maxCI)
            .union(minCI)
            .union(avgCfe)
            .union(minCfe)
            .union(maxCfe)
            .saveAsTextFile(resultsPath);
    }

    private JavaPairRDD<Integer, Double> calculateAvg(JavaPairRDD<Integer, Double> rdd) {
        return rdd.mapToPair(this::addOccurrence)
                .reduceByKey(this::sum2D)
                .mapToPair(this::getAvg);
    }

    private JavaPairRDD<Integer, Double> calculateMax(JavaPairRDD<Integer, Double> rdd) {
        return rdd.reduceByKey(Math::max);
    }

    private JavaPairRDD<Integer, Double> calculateMin(JavaPairRDD<Integer, Double> rdd) {
        return rdd.reduceByKey(Math::min);
    }

    private Tuple2<Integer, Double> getCIDPair(String line) {
        var fields = line.split(",");
        return new Tuple2<>(getYear(fields), getCID(fields));
    }

    private Tuple2<Integer, Double> getCFEPair(String line) {
        var fields = line.split(",");
        return new Tuple2<>(getYear(fields), getCFE(fields));
    }

    private Tuple2<Integer, Tuple2<Double, Integer>> addOccurrence(Tuple2<Integer, Double> pair) {
        return new Tuple2<>(pair._1(), new Tuple2<>(pair._2(), 1));
    }

    private Tuple2<Double, Integer> sum2D(Tuple2<Double, Integer> x, Tuple2<Double, Integer> y) {
        return new Tuple2<>(x._1() + y._1(), x._2() + y._2());
    }

    private Tuple2<Integer, Double> getAvg(Tuple2<Integer, Tuple2<Double, Integer>> x) {
        return new Tuple2<>(x._1(), x._2()._1() / x._2()._2());
    }

    private double getCFE(String[] fields) {
        return Double.parseDouble(fields[CsvFields.CFE_PERCENTAGE]);
    }

    private double getCID(String[] fields) {
        return Double.parseDouble(fields[CsvFields.CARBON_INTENSITY_DIRECT]);
    }

    private Integer getYear(String[] fields) {
        return LocalDateTime.parse(fields[CsvFields.DATETIME_UTC], DateTimeFormatter.ofPattern(CsvFields.DATETIME_FORMAT)).getYear();
    }
}
