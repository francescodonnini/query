package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

public class SecondFieldComparator implements NthDoubleComparator {
    @Override
    public int compare(Tuple2<Double, Double> o1, Tuple2<Double, Double> o2) {
        return o2._2().compareTo(o1._2());
    }
}
