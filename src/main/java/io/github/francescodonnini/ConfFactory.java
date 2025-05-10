package io.github.francescodonnini;

import java.util.Optional;

public class ConfFactory {
    private ConfFactory() {}

    public static Optional<Conf> getConf(String appName) {
        var sparkMaster = System.getenv("SPARK_MASTER");
        if (sparkMaster == null) {
            return Optional.empty();
        }
        var o = tryParseInt(System.getenv("SPARK_MASTER_PORT"));
        if (o.isEmpty()) {
            return Optional.empty();
        }
        var hdfsHost = System.getenv("HDFS_HOST");
        if (hdfsHost == null) {
            return Optional.empty();
        }
        var filePath = System.getenv("HDFS_PATH");
        if (filePath == null) {
            return Optional.empty();
        }
        var hdfsPort = tryParseInt(System.getenv("HDFS_PORT"));
        return hdfsPort.map(port -> new Conf(
                sparkMaster,
                o.get(),
                appName,
                hdfsHost,
                port,
                filePath
        ));
    }

    private static Optional<Integer> tryParseInt(String port) {
        if (port == null) {
            return Optional.empty();
        }
        return Optional.of(Integer.parseInt(port));
    }
}
