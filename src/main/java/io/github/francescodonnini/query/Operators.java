package io.github.francescodonnini.query;

import scala.Tuple2;
import scala.Tuple3;

public class Operators {
    private Operators() {}

    public static Tuple3<Double, Double, Integer> sum3(
            Tuple3<Double, Double, Integer> x,
            Tuple3<Double, Double, Integer> y) {
        return new Tuple3<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3());
    }

    public static <K> Tuple2<K, Tuple2<Double, Double>> average3(
            Tuple2<K, Tuple3<Double, Double, Integer>> x) {
        return new Tuple2<>(x._1(), new Tuple2<>(x._2()._1() / x._2()._3(), x._2()._2() / x._2()._3()));
    }
}