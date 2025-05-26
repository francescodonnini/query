package io.github.francescodonnini.query;

import scala.Tuple2;

public class Operators {
    private Operators() {}

    public static Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> sumDoubleIntPair(
            Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> x,
            Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> y) {
        return new Tuple2<>(
                new Tuple2<>(x._1()._1() + y._1()._1(), x._1()._2() + y._1()._2()),
                new Tuple2<>(x._2()._1() + y._2()._1(), x._2()._2() + y._2()._2()));
    }

    public static <K> Tuple2<K, Tuple2<Double, Double>> average(
            Tuple2<K, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> x) {
        return new Tuple2<>(x._1(), new Tuple2<>(x._2()._1()._1() / x._2()._1()._2(), x._2()._2()._1() / x._2()._2()._2()));
    }
}