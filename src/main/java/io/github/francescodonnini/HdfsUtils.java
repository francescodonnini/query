package io.github.francescodonnini;

import java.io.Serializable;
import java.nio.file.Path;

public class HdfsUtils implements Serializable {
    private HdfsUtils() {}

    public static String getHdfsPath(Conf conf) {
        return String.format("hdfs://%s:%d", conf.getHdfsHost(), conf.getHdfsPort());
    }

    public static String getDatasetPath(Conf conf) {
        return String.format("%s/%s", getHdfsPath(conf), conf.getFilePath());
    }

    public static String createPath(Conf conf, String... components) {
        return Path.of(getHdfsPath(conf), components).toString();
    }
}
