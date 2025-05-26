package io.github.francescodonnini.data;

import java.io.Serializable;

public enum CsvField implements Serializable {
    DATETIME_UTC(0, "Datetime (UTC)"),
    COUNTRY(1, "Country"),
    ZONE_NAME(2, "Zone name"),
    ZONE_ID(3, "Zone id"),
    CARBON_INTENSITY_DIRECT(4, "Carbon intensity gCO₂eq/kWh (direct)"),
    CARBON_INTENSITY_LIFE_CYCLE(5, "Carbon intensity gCO₂eq/kWh (Life cycle)"),
    CFE_PERCENTAGE(6, "Carbon-free energy percentage (CFE%)"),
    RE_PERCENTAGE(7, "Renewable energy percentage (RE%)"),
    DATA_SOURCE(8, "Data source"),
    DATA_ESTIMATED(9, "Data estimated"),
    DATA_ESTIMATION_METHOD(10, "Data estimation method");

    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final int index;
    private final String header;

    CsvField(int index, String header) {
        this.index = index;
        this.header = header;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return header;
    }
}
