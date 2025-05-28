package io.github.francescodonnini.data;

public enum ParquetField {
    DATETIME_UTC("datetime_utc"),
    ZONE_ID("zone_id"),
    CARBON_INTENSITY_DIRECT("carbon_intensity_gco2eq_per_kwh_direct"),
    CFE_PERCENTAGE("carbon_free_energy_percentage_cfe");

    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final String name;

    ParquetField(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
