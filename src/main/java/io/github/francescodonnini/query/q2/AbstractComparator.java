package io.github.francescodonnini.query.q2;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

interface AbstractComparator extends Comparator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>, Serializable {}