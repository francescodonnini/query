package io.github.francescodonnini;

import io.github.francescodonnini.conf.Conf;
import org.apache.spark.sql.SparkSession;

public class SparkFactory {
    private SparkFactory() {}

    public static SparkSession getSparkSession(Conf conf) {
        return SparkSession.builder()
                .master(String.format("spark://%s:%d", conf.getString("SPARK_MASTER_HOST"), conf.getInt("SPARK_MASTER_PORT")))
                .appName(conf.getString("SPARK_APP_NAME"))
                .getOrCreate();
    }
}
