package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

public class FirstFieldComparator implements NthDoubleComparator {
    @Override
    public int compare(Tuple2<Double, Double> o1, Tuple2<Double, Double> o2) {
        return o1._1().compareTo(o2._1());
    }
}
