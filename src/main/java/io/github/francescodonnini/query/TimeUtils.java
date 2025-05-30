package io.github.francescodonnini.query;

import java.time.*;

public class TimeUtils {
    private TimeUtils() {}

    public static Instant fromYear(int year) {
        return Year.of(year)
                .atDay(1)
                .atStartOfDay()
                .toInstant(ZoneOffset.UTC);
    }

    public static Instant fromYearAndMonth(int year, int month) {
        return LocalDate.of(year, month, 1)
                .atStartOfDay()
                .toInstant(ZoneOffset.UTC);
    }

    public static Instant fromYearAndMonth(String yearAndMonth) {
        return YearMonth.parse(yearAndMonth)
                .atDay(1)
                .atStartOfDay()
                .toInstant(ZoneOffset.UTC);
    }

    public static Instant fromYearAndDayOfYear(int year, int dayOfYear) {
        return LocalDate.ofYearDay(year, dayOfYear)
                .atStartOfDay()
                .toInstant(ZoneOffset.UTC);
    }
}
