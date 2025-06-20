package io.github.francescodonnini;

import io.github.francescodonnini.conf.Conf;

import java.io.Serializable;
import java.nio.file.Path;

public class HdfsUtils implements Serializable {
    private HdfsUtils() {}

    public static String getHdfsPath(Conf conf) {
        return String.format("hdfs://%s:%d", conf.getString("HDFS_HOST"), conf.getInt("HDFS_PORT"));
    }

    public static String createPath(Conf conf, String... components) {
        return getHdfsPath(conf) + Path.of("/", components);
    }
}
