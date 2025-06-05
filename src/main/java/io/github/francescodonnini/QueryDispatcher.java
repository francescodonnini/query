package io.github.francescodonnini;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.francescodonnini.cli.Command;
import io.github.francescodonnini.cli.QueryKind;
import io.github.francescodonnini.conf.Conf;
import io.github.francescodonnini.conf.ConfFactory;
import io.github.francescodonnini.query.*;
import io.github.francescodonnini.query.q1.FirstQueryDF;
import io.github.francescodonnini.query.q1.FirstQueryRDD;
import io.github.francescodonnini.query.q1.FirstQuerySQL;
import io.github.francescodonnini.query.q2.SecondQueryDF;
import io.github.francescodonnini.query.q2.SecondQueryRDD;
import io.github.francescodonnini.query.q2.SecondQueryRDDZipped;
import io.github.francescodonnini.query.q2.SecondQuerySQL;
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
        try (var influx = factory.create();
             var query = createQuery(conf, command)) {
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
        var inputPath = getDatasetPath(conf, command.getQueryKind());
        var outputPath = getOutputPath(conf);
        var factory = getInfluxDbFactory(conf);
        var spark = SparkFactory.getSparkSession(conf);
        var save = command.getTime().isEmpty();
        switch (command.getQueryKind()) {
            case Q1_DF:
                return new FirstQueryDF(spark, inputPath, save, outputPath, factory);
            case Q1_RDD:
                return new FirstQueryRDD(spark, inputPath, outputPath, factory, save);
            case Q1_SQL:
                return new FirstQuerySQL(spark, inputPath, save, outputPath, factory);
            case Q2_DF:
                return new SecondQueryDF(spark, inputPath, outputPath, factory, save);
            case Q2_RDD:
                return new SecondQueryRDD(spark, inputPath, outputPath, factory, save);
            case Q2_SQL:
                return new SecondQuerySQL(spark, inputPath, save, outputPath, factory);
            case Q2_ZIPPED:
                return new SecondQueryRDDZipped(spark, inputPath, outputPath, factory, save);
            case Q3_RDD:
                return new ThirdQueryRDD(spark, inputPath, outputPath, factory, save);
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

    private static String getDatasetPath(Conf conf, QueryKind kind) {
        String path;
        if (System.getenv("USE_IN_MEMORY_DATASET") != null) {
            path ="file://" + "/opt" + "/dataset/" + "/data";
        } else {
            path = HdfsUtils.createPath(conf, conf.getString("HDFS_PATH"));
        }
        return path + getFormat(kind);
    }

    private static String getFormat(QueryKind kind) {
        switch (kind) {
            case Q1_DF:
            case Q2_DF:
            case Q3_DF:
                return ".parquet";
            case Q1_RDD:
            case Q2_RDD:
            case Q2_ZIPPED:
            case Q3_RDD:
                return ".csv";
            default:
                throw new IllegalArgumentException("invalid query " + kind);
        }
    }

    private static String getOutputPath(Conf conf) {
        return HdfsUtils.createPath(conf, "user", "spark", conf.getString("SPARK_APP_NAME"));
    }

    private static Command parse(String[] args) {
        var command = new Command();
        new CommandLine(command).execute(args);
        return command;
    }
}