package io.github.francescodonnini.query.q3;

import io.github.francescodonnini.query.Query;
import org.apache.spark.sql.SparkSession;

public class ThirdQueryDF implements Query {
    private final SparkSession spark;
    private final String datasetPath;
    private final String resultsPath;

    public ThirdQueryDF(SparkSession spark, String datasetPath, String resultsPath) {
        this.spark = spark;
        this.datasetPath = datasetPath;
        this.resultsPath = resultsPath;
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
        throw new UnsupportedOperationException("not yet implemented");
    }
}
