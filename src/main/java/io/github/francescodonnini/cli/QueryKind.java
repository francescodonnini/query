package io.github.francescodonnini.cli;

import java.io.Serializable;

public enum QueryKind implements Serializable {
    Q1_DF, Q1_RDD, Q2_DF, Q2_RDD, Q2_ZIPPED, Q3_DF, Q3_RDD;

    public static QueryKind fromString(String s) {
        switch (s) {
            case "1D":
                return Q1_DF;
            case "1R":
                return Q1_RDD;
            case "2D":
                return Q2_DF;
            case "2R":
                return Q2_RDD;
            case "2Z":
                return Q2_ZIPPED;
            case "3D":
                return Q3_DF;
            case "3R":
                return Q3_RDD;
            default:
                throw new IllegalArgumentException("invalid query number: expected one of 1D, 1R, 2D, 2R, 2Z, 3D, 3R but got " + s);
        }
    }
}