package io.github.francescodonnini;

public class HdfsUtils {
    private HdfsUtils() {}

    public static String getDatasetPath(Conf conf) {
        return String.format("hdfs://%s:%d/%s", conf.getHdfsHost(), conf.getHdfsPort(), conf.getFilePath());
    }
}
