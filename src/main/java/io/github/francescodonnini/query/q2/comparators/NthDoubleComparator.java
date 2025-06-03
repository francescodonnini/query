package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public abstract class NthDoubleComparator implements Comparator<Tuple2<Double, Double>>, Serializable {
    protected final boolean ascending;

    protected NthDoubleComparator(boolean ascending) {
        this.ascending = ascending;
    }
}
