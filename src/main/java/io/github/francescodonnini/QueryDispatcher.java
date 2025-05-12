package io.github.francescodonnini;

import io.github.francescodonnini.query.FirstQuery;
import io.github.francescodonnini.query.SecondQuery;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryDispatcher {
    private static final Logger logger = Logger.getLogger(QueryDispatcher.class.getName());
    public static void main(String[] args) {
        var optionalQueryNumber = tryParseQueryNumber(args);
        if (optionalQueryNumber.isEmpty()) {
            System.exit(1);
        }
        int queryNumber = optionalQueryNumber.get();
        var o = ConfFactory.getConf("FirstQuery");
        if (o.isEmpty()) {
            logger.log(Level.SEVERE, "some configuration parameters are missing");
            System.exit(1);
        }
        var conf = o.get();
        if (queryNumber == 1) {
            executeFirstQuery(conf);
        } else if (queryNumber == 2) {
            executeSecondQuery(conf);
        } else {
            logger.log(Level.SEVERE, () -> "unexpected query number " + queryNumber);
            System.exit(1);
        }
    }

    private static void executeFirstQuery(Conf conf) {
        var spark = SparkFactory.getSparkSession(conf);
        try (var query = new FirstQuery(spark, getDatasetPath(conf), getResultsPath(conf, "firstQuery"))) {
            query.submit();
        }
    }

    private static void executeSecondQuery(Conf conf) {
        var spark = SparkFactory.getSparkSession(conf);
        try (var query = new SecondQuery(spark, getDatasetPath(conf), getResultsPath(conf, "secondQuery"))) {
            query.submit();
        }
    }

    private static String getDatasetPath(Conf conf) {
        return HdfsUtils.createPath(conf, conf.getFilePath());
    }

    private static String getResultsPath(Conf conf, String query) {
        return HdfsUtils.createPath(conf, "user", "spark", query);
    }

    private static Optional<Integer> tryParseQueryNumber(String[] args) {
        if (args.length != 1) {
            logger.log(Level.SEVERE, "expected at least one argument");
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(args[0]));
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE, "failed to parse query number", e);
            return Optional.empty();
        }
    }
}