package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

public class FirstFieldComparator extends NthDoubleComparator {
    public FirstFieldComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compare(Tuple2<Double, Double> o1, Tuple2<Double, Double> o2) {
        if (ascending) {
            return o1._1().compareTo(o2._1());
        }
        return o2._1().compareTo(o1._1());
    }
}
