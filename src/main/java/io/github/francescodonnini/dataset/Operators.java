package io.github.francescodonnini.dataset;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.PatternSyntaxException;

public class Operators {
    private Operators() {}

    public static double getDoubleField(String line, int field) {
        return Double.parseDouble(line.split(",")[field]);
    }

    public static Optional<Double> getDoubleFieldSafe(String line, int field) {
        try {
            return Optional.of(getDoubleField(line, field));
        } catch (NumberFormatException | PatternSyntaxException ignored) {
            return Optional.empty();
        }
    }

    public static int getIntField(String line, int field) {
        return Integer.parseInt(line.split(",")[field]);
    }

    public static Optional<Integer> getIntFieldSafe(String line, int field) {
        try {
            return Optional.of(getIntField(line, field));
        } catch (NumberFormatException | PatternSyntaxException ignored) {
            return Optional.empty();
        }
    }

    public static LocalDateTime getLocalDateTime(String line, int field, String pattern) {
        return LocalDateTime.parse(line.split(",")[field], DateTimeFormatter.ofPattern(pattern));
    }

    public static Optional<LocalDateTime> getLocalDateTimeSafe(String line, int field, String pattern) {
        try {
            return Optional.of(getLocalDateTime(line, field, pattern));
        } catch (IllegalArgumentException | DateTimeParseException ignored) {
            return Optional.empty();
        }
    }
}
