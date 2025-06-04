package io.github.francescodonnini.data;

import java.io.Serializable;

public enum CsvField implements Serializable {
    DATETIME_UTC(0, "Datetime (UTC)"),
    ZONE_ID(1, "Zone id"),
    CARBON_INTENSITY_DIRECT(2, "Carbon intensity gCO₂eq/kWh (direct)"),
    CFE_PERCENTAGE(3, "Carbon-free energy percentage (CFE%)");

    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final int index;
    private final String columnName;

    CsvField(int index, String columnName) {
        this.index = index;
        this.columnName = columnName;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return columnName;
    }
}
