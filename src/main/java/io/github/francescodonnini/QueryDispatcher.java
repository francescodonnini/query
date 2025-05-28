package io.github.francescodonnini;

import com.influxdb.client.write.Point;
import io.github.francescodonnini.cli.Command;
import io.github.francescodonnini.cli.QueryKind;
import io.github.francescodonnini.conf.Conf;
import io.github.francescodonnini.conf.ConfFactory;
import io.github.francescodonnini.query.*;
import io.github.francescodonnini.query.q1.FirstQueryDF;
import io.github.francescodonnini.query.q1.FirstQueryRDD;
import io.github.francescodonnini.query.q2.SecondQueryDF;
import io.github.francescodonnini.query.q2.SecondQueryRDD;
import io.github.francescodonnini.query.q3.ThirdQueryDF;
import io.github.francescodonnini.query.q3.ThirdQueryRDD;
import picocli.CommandLine;

import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryDispatcher {
    private static final Logger logger = Logger.getLogger(QueryDispatcher.class.getName());

    public static void main(String[] args) {
        var result = parse(args);
        var o = ConfFactory.getConf(getAppName(result));
        if (o.isEmpty()) {
            logger.log(Level.SEVERE, "some configuration parameters are missing");
            System.exit(1);
        }
        var conf = o.get();
        var time = result.getTime();
        if (time.isEmpty()) {
            executeQuery(conf, result.getQueryKind(), result.useRDD());
        } else {
            timeQuery(o.get(), result.getQueryKind(), result.useRDD(), time.get());
        }
    }

    private static String getAppName(Command command) {
        return "query-" +
                command.getQueryKind().name().toLowerCase() +
                "-" +
                (command.useRDD() ? "rdd" : "df") +
                "-" +
                System.currentTimeMillis();
    }

    private static void timeQuery(Conf conf, QueryKind queryKind, boolean useRDD, int numOfRuns) {
        var factory = getInfluxDbFactory(conf);
        try (var query = createQuery(conf, queryKind, useRDD);
             var influx = factory.create()) {
            var writer = influx.getWriteApiBlocking();
            for (var i = 0; i < numOfRuns; ++i) {
                var start = System.nanoTime();
                query.submit();
                var duration = System.nanoTime() - start;
                writer.writePoint(Point.measurement("time-" + queryKind.name())
                        .addField("duration", duration));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to execute query", e);
            System.exit(1);
        }
    }

    private static void executeQuery(Conf conf, QueryKind queryKind, boolean useRDD) {
        try (var query = createQuery(conf, queryKind, useRDD)) {
            query.submit();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to execute query", e);
            System.exit(1);
        }
    }

    private static Query createQuery(Conf conf, QueryKind queryKind, boolean useRDD) {
        var datasetPath = getDatasetPath(conf);
        var resultsPath = getResultsPath(conf);
        var factory = getInfluxDbFactory(conf);
        switch (queryKind) {
            case Q1:
                if (useRDD) {
                    return new FirstQueryRDD(SparkFactory.getSparkSession(conf), datasetPath, resultsPath, factory);
                }
                return new FirstQueryDF(SparkFactory.getSparkSession(conf), datasetPath, resultsPath, factory);
            case Q2:
                if (useRDD) {
                    return new SecondQueryRDD(SparkFactory.getSparkSession(conf), datasetPath, resultsPath, factory);
                }
                return new SecondQueryDF(SparkFactory.getSparkSession(conf),
                                         datasetPath,
                                         resultsPath, factory);
            case Q3:
                if (useRDD) {
                    return new ThirdQueryRDD(SparkFactory.getSparkSession(conf), datasetPath, resultsPath);
                }
                return new ThirdQueryDF(SparkFactory.getSparkSession(conf), datasetPath, resultsPath);
            default:
                throw new IllegalArgumentException("invalid query " + queryKind);
        }
    }

    private static InfluxDbWriterFactory getInfluxDbFactory(Conf conf) {
        return new InfluxDbWriterFactoryImpl()
                .setHost(conf.getString("INFLUXDB_HOST"))
                .setPort(conf.getInt("INFLUXDB_PORT"))
                .setUsername(conf.getString("INFLUXDB_USER"))
                .setPassword(conf.getString("INFLUXDB_PASSWORD"))
                .setToken(conf.getString("INFLUXDB_TOKEN"))
                .setOrg(conf.getString("INFLUXDB_ORG"))
                .setBucket(conf.getString("INFLUXDB_BUCKET"));
    }

    private static String getDatasetPath(Conf conf) {
        return HdfsUtils.createPath(conf, conf.getString("HDFS_PATH"));
    }

    private static String getResultsPath(Conf conf) {
        return HdfsUtils.createPath(conf, "user", "spark", conf.getString("SPARK_APP_NAME"));
    }

    private static Command parse(String[] args) {
        var command = new Command();
        new CommandLine(command).execute(args);
        return command;
    }
}