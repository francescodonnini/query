package io.github.francescodonnini.cli;

public enum QueryKind {
    Q1, Q2, Q3;

    public static QueryKind fromInt(int i) {
        switch (i) {
            case 1:
                return Q1;
            case 2:
                return Q2;
            case 3:
                return Q3;
            default:
                throw new IllegalArgumentException("invalid query number: expected 1, 2 or 3 but got " + i);
        }
    }
}