package io.github.francescodonnini;

import io.github.francescodonnini.conf.Conf;
import io.github.francescodonnini.conf.ConfFactory;
import io.github.francescodonnini.query.*;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryDispatcher {
    public static class QueryArgs {
        public enum QueryKind {
            Q1, Q2, Q3;

            public static QueryKind fromInt(int i) {
                switch (i) {
                    case 1: return Q1;
                    case 2: return Q2;
                    case 3: return Q3;
                    default: throw new IllegalArgumentException("invalid query number: expected 1, 2 or 3 but got " + i);
                }
            }
        }
        private final QueryKind queryKind;
        private final boolean useRDD;

        public QueryArgs(int queryNumber, boolean useRDD) {
            this.queryKind = QueryKind.fromInt(queryNumber);
            this.useRDD = useRDD;
        }

        public QueryKind getQueryKind() {
            return queryKind;
        }

        public boolean useRDD() {
            return useRDD;
        }
    }

    private static final Logger logger = Logger.getLogger(QueryDispatcher.class.getName());

    public static void main(String[] args) {
        var oArgs = tryParseQueryArgs(args);
        if (oArgs.isEmpty()) {
            System.exit(1);
        }
        var queryArgs = oArgs.get();
        var o = ConfFactory.getConf(getAppName(queryArgs));
        if (o.isEmpty()) {
            logger.log(Level.SEVERE, "some configuration parameters are missing");
            System.exit(1);
        }
        executeQuery(o.get(), queryArgs);
    }

    private static String getAppName(QueryArgs queryArgs) {
        return "query-" +
                queryArgs.getQueryKind() +
                "-" +
                (queryArgs.useRDD() ? "rdd" : "df") +
                "-" +
                System.currentTimeMillis();
    }

    private static void executeQuery(Conf conf, QueryArgs args) {
        try (var query = createQuery(conf, args)) {
            query.submit();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to execute query", e);
            System.exit(1);
        }
    }

    private static Query createQuery(Conf conf, QueryArgs args) {
        var datasetPath = getDatasetPath(conf);
        var resultsPath = getResultsPath(conf);
        switch (args.getQueryKind()) {
            case Q1:
                if (args.useRDD()) {
                    return new FirstQueryRDD(SparkFactory.getSparkSession(conf), datasetPath, resultsPath);
                }
                return new FirstQueryDF(SparkFactory.getSparkSession(conf), datasetPath, resultsPath);
            case Q2:
                if (args.useRDD()) {
                    return new SecondQueryRDD(SparkFactory.getSparkSession(conf), datasetPath, resultsPath);
                }
                return new SecondQueryDF(SparkFactory.getSparkSession(conf),
                                         datasetPath,
                                         getInfluxDbUrl(conf),
                                         conf.getString("INFLUXDB_USER"),
                                         conf.getString("INFLUXDB_PASSWORD"),
                                         conf.getString("INFLUXDB_TOKEN"),
                                         conf.getString("INFLUXDB_ORG"),
                                         conf.getString("INFLUXDB_BUCKET"),
                                         resultsPath);
            case Q3:
                if (args.useRDD()) {
                    return new ThirdQueryRDD(SparkFactory.getSparkSession(conf), datasetPath, resultsPath);
                }
                return new ThirdQueryDF(SparkFactory.getSparkSession(conf), datasetPath, resultsPath);
            default:
                throw new IllegalArgumentException("invalid query " + args.getQueryKind());
        }
    }

    private static String getInfluxDbUrl(Conf conf) {
        return String.format("http://%s:%d", conf.getString("INFLUXDB_HOST"), conf.getInt("INFLUXDB_PORT"));
    }

    private static String getDatasetPath(Conf conf) {
        return HdfsUtils.createPath(conf, conf.getString("HDFS_PATH"));
    }

    private static String getResultsPath(Conf conf) {
        return HdfsUtils.createPath(conf, "user", "spark", conf.getString("SPARK_APP_NAME"));
    }

    private static Optional<QueryArgs> tryParseQueryArgs(String[] args) {
        if (args.length != 2) {
            logger.log(Level.SEVERE, "usage: <query number> <use RDD>");
            return Optional.empty();
        }
        try {
            var queryNumber = parseQueryNumber(args[0]);
            var useRDD = useRDD(args[1]);
            return Optional.of(new QueryArgs(queryNumber, useRDD) );
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE, "failed to parse <query number>", e);
        } catch (IllegalArgumentException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return Optional.empty();
    }

    private static int parseQueryNumber(String arg) {
        var queryNumber = Integer.parseInt(arg);
        if (queryNumber < 1 || queryNumber > 3) {
            throw new IllegalArgumentException("failed to parse <query number>: expected a number between 1 and 3 but got " + arg);
        }
        return queryNumber;
    }

    private static boolean useRDD(String arg) {
        switch (arg) {
            case "D":
                return false;
            case "R":
                return true;
            default:
                throw new IllegalArgumentException("failed to parse <use RDD>: expected one of \"D\" or \"R\" but got " + arg);
        }
    }
}