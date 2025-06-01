package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class IntPairComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
        var cmp = Integer.compare(o1._1(), o2._1());
        if (cmp == 0) {
            return Integer.compare(o1._2(), o2._2());
        }
        return cmp;
    }
}
