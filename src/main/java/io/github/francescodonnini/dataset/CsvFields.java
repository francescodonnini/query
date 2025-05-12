package io.github.francescodonnini.dataset;

import java.io.Serializable;

public class CsvFields implements Serializable {
    // Indice del campo "Datetime (UTC)"
    public static final int DATETIME_UTC = 0;
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    // Indice del campo "Country"
    public static final int COUNTRY = 1;

    // Indice del campo "Zone name"
    public static final int ZONE_NAME = 2;

    // Indice del campo "Zone id"
    public static final int ZONE_ID = 3;

    // Indice del campo "Carbon intensity gCO₂eq/kWh (direct)"
    public static final int CARBON_INTENSITY_DIRECT = 4;

    // Indice del campo "Carbon intensity gCO₂eq/kWh (Life cycle)"
    public static final int CARBON_INTENSITY_LIFE_CYCLE = 5;

    // Indice del campo "Carbon-free energy percentage (CFE%)"
    public static final int CFE_PERCENTAGE = 6;

    // Indice del campo "Renewable energy percentage (RE%)"
    public static final int RE_PERCENTAGE = 7;

    // Indice del campo "Data source"
    public static final int DATA_SOURCE = 8;

    // Indice del campo "Data estimated"
    public static final int DATA_ESTIMATED = 9;

    // Indice del campo "Data estimation method"
    public static final int DATA_ESTIMATION_METHOD = 10;
}

