package io.github.francescodonnini.conf;

import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConfFactory {
    private static final Logger logger = Logger.getLogger(ConfFactory.class.getName());

    private ConfFactory() {}

    public static Optional<Conf> getConf(String appName) {
        var properties = new Properties();
        properties.setProperty("SPARK_APP_NAME", appName);
        try {
            setString(properties, "SPARK_MASTER_HOST");
            setString(properties, "SPARK_MASTER_PORT");
            setString(properties, "HDFS_HOST");
            setInt(properties, "HDFS_PORT");
            setString(properties, "HDFS_PATH");
            setString(properties, "INFLUXDB_HOST");
            setInt(properties, "INFLUXDB_PORT");
            setString(properties, "INFLUXDB_USER");
            setString(properties, "INFLUXDB_PASSWORD");
            setString(properties, "INFLUXDB_TOKEN");
            setString(properties, "INFLUXDB_ORG");
            setString(properties, "INFLUXDB_BUCKET");
            return Optional.of(new ConfImpl(properties));
        } catch (IllegalStateException e) {
            logger.log(Level.SEVERE, e.getMessage());
            return Optional.empty();
        }
    }

    private static void setString(Properties properties, String name) {
        var value = System.getenv(name);
        if (value == null) {
            throw new IllegalStateException("missing environment variable " + name);
        }
        properties.setProperty(name, value);
    }

    private static void setInt(Properties properties, String name) {
        var influxDbPort = tryParseInt(System.getenv(name));
        if (influxDbPort.isEmpty()) {
            throw new IllegalStateException("missing environment variable " + name);
        }
        properties.setProperty(name, influxDbPort.get().toString());
    }

    private static Optional<Integer> tryParseInt(String port) {
        if (port == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(port));
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE, () -> "failed to parse " + port + " as an integer");
            return Optional.empty();
        }
    }
}
