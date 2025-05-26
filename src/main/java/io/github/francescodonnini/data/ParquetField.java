package io.github.francescodonnini.data;

public enum ParquetField {
    DATETIME_UTC("datetime_utc"),
    COUNTRY("country"),
    ZONE_NAME("zone_name"),
    ZONE_ID("zone_id"),
    CARBON_INTENSITY_DIRECT("carbon_intensity_gco2eq_per_kwh_direct"),
    CARBON_INTENSITY_LIFE_CYCLE("carbon_intensity_gco2eq_per_kwh_life_cycle"),
    CFE_PERCENTAGE("carbon_free_energy_percentage_cfe"),
    RE_PERCENTAGE("renewable_energy_percentage_re"),
    DATA_SOURCE("data_source"),
    DATA_ESTIMATED("data_estimated"),
    DATA_ESTIMATION_METHOD("data_estimation_method");

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
