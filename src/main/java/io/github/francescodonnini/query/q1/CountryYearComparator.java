package io.github.francescodonnini.query.q1;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class CountryYearComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        var cmp = o1._1().compareTo(o2._1());
        if (cmp == 0) {
            return Integer.compare(o1._2(), o2._2());
        }
        return cmp;
    }
}
