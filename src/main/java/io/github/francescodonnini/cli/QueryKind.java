package io.github.francescodonnini.cli;

import java.io.Serializable;

public enum QueryKind implements Serializable {
    Q1_DF, Q1_RDD, Q1_SQL, Q2_DF, Q2_RDD, Q2_SQL, Q2_ZIPPED, Q3_DF, Q3_RDD;

    public static QueryKind fromString(String s) {
        switch (s) {
            case "1D":
                return Q1_DF;
            case "1R":
                return Q1_RDD;
            case "1S":
                return Q1_SQL;
            case "2D":
                return Q2_DF;
            case "2R":
                return Q2_RDD;
            case "2S":
                return Q2_SQL;
            default:
                throw new IllegalArgumentException("invalid query number: expected one of 1D, 1R, 2D, 2R but got " + s);
        }
    }
}