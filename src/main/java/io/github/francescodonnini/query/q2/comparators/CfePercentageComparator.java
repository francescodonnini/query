package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

public class CfePercentageComparator implements AbstractComparator {
    private static class AscendingCfePercentageComparator implements AbstractComparator {
        @Override
        public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
            return Double.compare(o1._2()._2(), o2._2()._2());
        }
    }
    private static class DescendingCfePercentageComparator implements AbstractComparator {
        @Override
        public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
            return Double.compare(o2._2()._2(), o1._2()._2());
        }
    }
    private final AbstractComparator strategy;

    public CfePercentageComparator(boolean asc) {
        if (asc) {
            strategy = new AscendingCfePercentageComparator();
        } else {
            strategy = new DescendingCfePercentageComparator();
        }
    }

    @Override
    public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
        return strategy.compare(o1, o2);
    }
}
