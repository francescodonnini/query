package io.github.francescodonnini.query;

import org.apache.spark.sql.SparkSession;

public abstract class AbstractQuery implements Query {
    private final String appName;
    private final SparkSession spark;
    private final String inputPath;
    private final boolean save;

    protected AbstractQuery(SparkSession spark, String inputPath, boolean save) {
        this.appName = spark.sparkContext().appName();
        this.spark = spark;
        this.inputPath = inputPath;
        this.save = save;
    }

    @Override
    public void close() {
        spark.stop();
    }

    protected String getAppName() {
        return appName;
    }

    protected SparkSession getSparkSession() {
        return spark;
    }

    protected String getInputPath() {
        return inputPath;
    }

    protected boolean shouldSave() {
        return save;
    }
}
