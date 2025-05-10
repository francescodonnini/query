package io.github.francescodonnini.query;

import io.github.francescodonnini.Conf;
import io.github.francescodonnini.HdfsUtils;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

/**
 * Facendo riferimento al dataset dei valori energetici dell’Italia e della Svezia, aggregare i dati su base
 * annua. Calcolare la media, il minimo e il massimo di “Carbon intensity gCO2 eq/kWh (direct)” e
 * “Carbon-free energy percentage (CFE%)” per ciascun anno dal 2021 al 2024. Inoltre, considerando il
 * valor medio di “Carbon intensity gCO2eq/kWh (direct)” e “Carbon-free energy percentage (CFE%)”
 * aggregati su base annua, generare due grafici che consentano di confrontare visivamente l’andamento
 * per Italia e Svezia.
 */
public class FirstQuery implements Query {
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

    @Override
    public void submit() {
        var lines = spark.sparkContext().textFile(HdfsUtils.getDatasetPath(conf), 1);
        var count = lines.count();
        logger.info(() -> "Found " + count + " lines.");
    }
}
