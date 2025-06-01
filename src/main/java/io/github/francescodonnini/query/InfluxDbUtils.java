package io.github.francescodonnini.query;

import com.influxdb.client.write.Point;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;

public class InfluxDbUtils {
    private InfluxDbUtils() {}
    
    public static <T> void save(
            InfluxDbWriterFactory clientFactory,
            Iterator<T> partition,
            Function<T, Point> rowFactory) {
        try (var client = clientFactory.create()) {
            var points = new ArrayList<Point>();
            partition.forEachRemaining(row -> points.add(rowFactory.apply(row)));
            var writer = client.getWriteApiBlocking();
            writer.writePoints(points);
        }
    }
}
