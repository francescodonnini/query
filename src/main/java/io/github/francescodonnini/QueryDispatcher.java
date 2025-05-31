package io.github.francescodonnini;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.cli.Command;
import io.github.francescodonnini.conf.Conf;
import io.github.francescodonnini.conf.ConfFactory;
import io.github.francescodonnini.query.*;
import io.github.francescodonnini.query.q1.FirstQueryDF;
import io.github.francescodonnini.query.q1.FirstQueryRDD;
import io.github.francescodonnini.query.q2.SecondQueryDF;
import io.github.francescodonnini.query.q2.SecondQueryRDD;
import io.github.francescodonnini.query.q2.SecondQueryRDDV2;
import io.github.francescodonnini.query.q3.ThirdQueryDF;
import io.github.francescodonnini.query.q3.ThirdQueryRDD;
import picocli.CommandLine;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryDispatcher {
    private static final Logger logger = Logger.getLogger(QueryDispatcher.class.getName());

    public static void main(String[] args) {
        var command = parse(args);
        var o = ConfFactory.getConf(getAppName(command));
        if (o.isEmpty()) {
            logger.log(Level.SEVERE, "some configuration parameters are missing");
            System.exit(1);
        }
        var conf = o.get();
        var time = command.getTime();
        if (time.isEmpty()) {
            executeQuery(conf, command);
        } else {
            timeQuery(o.get(), command);
        }
    }

    private static String getAppName(Command command) {
        return command.getQueryKind().name();
    }

    private static void timeQuery(Conf conf, Command command) {
        var time = command.getTime();
        if (time.isEmpty()) {
            logger.log(Level.SEVERE, "timeQuery called but missing time parameter");
            return;
        }
        var tag = conf.getString("SPARK_APP_NAME");
        logger.log(Level.INFO, () -> String.format("timeQuery appName=%s, #runs=%d", tag, time.get()));
        var factory = getInfluxDbFactory(conf);
        try (var query = createQuery(conf, command);
             var influx = factory.create()) {
            var writer = influx.getWriteApiBlocking();
            var points = new ArrayList<Point>();
            for (var i = 0; i < time.get(); ++i) {
                var start = Instant.now();
                query.submit();
                var duration = Duration.between(start, Instant.now());
                points.add(Point.measurement("time")
                        .addField("duration", duration.toMillis())
                        .addTag("app", tag)
                        .time(start, WritePrecision.MS));
            }
            writer.writePoints(points);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to execute query", e);
            System.exit(1);
        }
    }

    private static void executeQuery(Conf conf, Command command) {
        try (var query = createQuery(conf, command)) {
            query.submit();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to execute query", e);
            System.exit(1);
        }
    }

    private static Query createQuery(Conf conf, Command command) {
        var datasetPath = getDatasetPath(conf);
        var resultsPath = getResultsPath(conf);
        var factory = getInfluxDbFactory(conf);
        var spark = SparkFactory.getSparkSession(conf);
        var save = command.getTime().isEmpty();
        switch (command.getQueryKind()) {
            case Q1_DF:
                return new FirstQueryDF(spark, datasetPath, resultsPath, factory, save);
            case Q1_RDD:
                return new FirstQueryRDD(spark, datasetPath, resultsPath, factory, save);
            case Q2_DF:
                return new SecondQueryDF(spark, datasetPath, resultsPath, factory, save);
            case Q2_RDD:
                return new SecondQueryRDD(spark, datasetPath, resultsPath, factory, save);
            case Q2_ZIPPED:
                return new SecondQueryRDDV2(spark, datasetPath, resultsPath, factory, save);
            case Q3_DF:
                return new ThirdQueryDF(spark, datasetPath, resultsPath);
            case Q3_RDD:
                return new ThirdQueryRDD(spark, datasetPath, resultsPath, factory);
            default:
                throw new IllegalArgumentException("invalid query " + command.getQueryKind());
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