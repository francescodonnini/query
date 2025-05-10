package io.github.francescodonnini;

import org.apache.spark.sql.SparkSession;

public class SparkFactory {
    private SparkFactory() {}

    public static SparkSession getSparkSession(Conf conf) {
        return SparkSession.builder()
                .master(String.format("spark://%s:%d", conf.getSparkMaster(), conf.getSparkMasterPort()))
                .appName(conf.getSparkAppName())
                .getOrCreate();
    }
}
