package io.github.francescodonnini.query.q3;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.data.CsvField;
import io.github.francescodonnini.query.InfluxDbWriterFactory;
import io.github.francescodonnini.query.Operators;
import io.github.francescodonnini.query.Query;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ThirdQueryRDD implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;
    private final InfluxDbWriterFactory factory;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CsvField.DATETIME_FORMAT);

    public ThirdQueryRDD(SparkSession spark, String datasetPath, String resultsPath, InfluxDbWriterFactory factory) {
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
     * Facendo riferimento al dataset dei valori energetici dell’Italia e della Svezia, aggregare i dati di cia-
     * scun paese sulle 24 ore della giornata, calcolando il valor medio di “Carbon intensity gCO2 eq/kWh
     * (direct)” e “Carbon-free energy percentage (CFE%)”. Calcolare il minimo, 25-esimo, 50-esimo, 75-
     * esimo percentile e massimo del valor medio di “Carbon intensity gCO2 eq/kWh (direct)” e “Carbon-
     * free energy percentage (CFE%)”. Inoltre, considerando il valor medio di “Carbon intensity gCO2eq/kWh
     * (direct)” e “Carbon-free energy percentage (CFE%)” aggregati sulle 24 fasce orarie giornaliere, ge-
     * nerare due grafici che consentano di confrontare visivamente l’andamento per Italia e Svezia.
     */
    @Override
    public void submit() {
        var lines = spark.sparkContext()
                .textFile(datasetPath + ".csv", 1)
                .toJavaRDD();
        var averages = lines.mapToPair(this::getPairWithOnes)
                .reduceByKey(Operators::sumDoubleIntPair)
                .map(Operators::average);
        save(averages);
        var cfeQuantiles = lines.mapToPair(this::getCfe)
                .groupByKey()
                .mapValues(this::toSortedList)
                .mapValues(this::getQuantiles);
        save(resultsPath + "/cfe.csv", cfeQuantiles);
        var ciQuantiles = lines.mapToPair(this::getCi)
                .groupByKey()
                .mapValues(this::toSortedList)
                .mapValues(this::getQuantiles);
        save(resultsPath + "/ci.csv", ciQuantiles);
    }

    private void save(String path, JavaPairRDD<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> quantiles) {
        quantiles.map(this::qToCsv)
                .saveAsTextFile(path);
    }

    private void save(JavaRDD<Tuple2<Integer, Tuple2<Double, Double>>> averages) {
        averages
                .map(this::toCsv)
                .saveAsTextFile(resultsPath + "/averages.csv");
        try (var client = factory.create() ) {
            var writer = client.getWriteApiBlocking();
            averages.map(this::toPoint)
                            .foreachPartition(p -> p.forEachRemaining(writer::writePoint));
        }
    }

    private Point toPoint(Tuple2<Integer, Tuple2<Double, Double>> avg) {
        return Point.measurement("q3-rdd-points")
                .addField("avgCi", avg._2()._1())
                .addField("avgCfe", avg._2()._2())
                .time(avg._1(), WritePrecision.S);
    }

    private String toCsv(Tuple2<Integer, Tuple2<Double, Double>> avg) {
        return avg._1() + "," + avg._2()._1() + "," + avg._2()._2();
    }

    private Tuple2<Integer, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> getPairWithOnes(String line) {
        var fields = getFields(line);
        return new Tuple2<>(getDay(fields), new Tuple2<>(getCiWithOne(fields), getCfeWithOne(fields)));
    }

    private int getDay(String[] fields) {
        var date = LocalDateTime.parse(getDatetime(fields), formatter);
        return date.getDayOfYear() * (date.getYear() - 2021);
    }

    private Tuple2<Double, Integer> getCiWithOne(String[] fields) {
        return new Tuple2<>(getCi(fields), 1);
    }

    private Tuple2<Double, Integer> getCfeWithOne(String[] fields) {
        return new Tuple2<>(getCfe(fields), 1);
    }

    private String qToCsv(Tuple2<Tuple2<String, Integer>, Tuple3<Double, Double, Double>> x) {
        return x._1()._1() + "," + x._1()._2() + "," + x._2()._1() + "," + x._2()._2() + "," + x._2()._3();
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
}
