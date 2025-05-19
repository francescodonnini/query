package io.github.francescodonnini.query;

import java.time.Instant;
import java.time.Year;
import java.time.ZonedDateTime;

public class TimeUtils {
    private TimeUtils() {}

    public static Instant fromYear(int year) {
        return Year.of(year)
                .atDay(1)
                .atStartOfDay()
                .toInstant(ZonedDateTime.now().getOffset());
    }
}
