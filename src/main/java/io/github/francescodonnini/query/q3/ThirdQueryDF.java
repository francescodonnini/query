package io.github.francescodonnini.query.q3;

import io.github.francescodonnini.query.AbstractQuery;
import org.apache.spark.sql.SparkSession;

public class ThirdQueryDF extends AbstractQuery {
    public ThirdQueryDF(SparkSession spark, String inputPath, boolean save) {
        super(spark, inputPath, save);
    }

    @Override
    public void submit() {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
