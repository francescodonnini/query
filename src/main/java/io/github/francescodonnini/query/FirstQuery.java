package io.github.francescodonnini.query;

import io.github.francescodonnini.Conf;
import io.github.francescodonnini.HdfsUtils;
import io.github.francescodonnini.dataset.CsvFields;
import io.github.francescodonnini.dataset.Operators;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.logging.Logger;

public class FirstQuery implements Query {
    private static class IntDoublePair {
        private final int intValue;
        private final double doubleValue;

        public IntDoublePair(int intValue, double doubleValue) {
            this.intValue = intValue;
            this.doubleValue = doubleValue;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public int getIntValue() {
            return intValue;
        }
    }
    private final Logger logger = Logger.getLogger(FirstQuery.class.getName());
    private final SparkSession spark;
    private final Conf conf;

    public FirstQuery(SparkSession spark, Conf conf) {
        this.spark = spark;
        this.conf = conf;
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
                .textFile(HdfsUtils.getDatasetPath(conf), 1)
                .toJavaRDD();
        var carbonIntensity = lines
                .map(this::getCIDPair)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce((x, y) -> x.doubleValue + y.doubleValue)
        var cfe = lines.map(this::getCFEPair)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private Optional<IntDoublePair> getCIDPair(String line) {
        var fields = line.split(",");
        var year = getYear(fields);
        var cfe = getCFE(fields);
        if (year.isEmpty() || cfe.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new IntDoublePair(year.get(), cfe.get()));

    }

    private Optional<IntDoublePair> getCFEPair(String line) {
        var fields = line.split(",");
        var year = getYear(fields);
        var cfe = getCID(fields);
        if (year.isEmpty() || cfe.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new IntDoublePair(year.get(), cfe.get()));

    }

    private Optional<Double> getCFE(String[] fields) {
        try {
            return Optional.of(Double.parseDouble(fields[CsvFields.CFE_PERCENTAGE]));
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException ignored) {
            return Optional.empty();
        }
    }

    private Optional<Double> getCID(String[] fields) {
        try {
            return Optional.of(Double.parseDouble(fields[CsvFields.CARBON_INTENSITY_DIRECT]));
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException ignored) {
            return Optional.empty();
        }
    }

    private Optional<Integer> getYear(String[] fields) {
        try {
            return Optional.of(LocalDateTime.parse(fields[CsvFields.DATETIME_UTC], DateTimeFormatter.ofPattern(CsvFields.DATETIME_FORMAT)).getYear());
        } catch (DateTimeParseException | ArrayIndexOutOfBoundsException ignored) {
            return Optional.empty();
        }
    }
}
