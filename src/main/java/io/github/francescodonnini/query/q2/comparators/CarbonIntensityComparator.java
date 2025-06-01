package io.github.francescodonnini.query.q2.comparators;

import scala.Tuple2;

public class CarbonIntensityComparator implements AbstractComparator {
    private static class AscendingCarbonIntensityComparator implements AbstractComparator {
        @Override
        public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
            return Double.compare(o1._2()._1(), o2._2()._1());
        }
    }
    private static class DescendingCarbonIntensityComparator implements AbstractComparator {
        @Override
        public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
            return Double.compare(o2._2()._1(), o1._2()._1());
        }
    }
    private final AbstractComparator strategy;

    public CarbonIntensityComparator(boolean asc) {
        if (asc) {
            strategy = new AscendingCarbonIntensityComparator();
        } else {
            strategy = new DescendingCarbonIntensityComparator();
        }
    }

    @Override
    public int compare(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o1, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o2) {
        return strategy.compare(o1, o2);
    }
}
