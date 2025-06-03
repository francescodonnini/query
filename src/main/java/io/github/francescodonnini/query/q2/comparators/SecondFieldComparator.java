package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

public class SecondFieldComparator extends NthDoubleComparator {
    public SecondFieldComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compare(Tuple2<Double, Double> o1, Tuple2<Double, Double> o2) {
        if (ascending) {
            return o1._2().compareTo(o2._2());
        }
        return o2._2().compareTo(o1._2());
    }
}
