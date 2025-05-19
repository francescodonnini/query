package io.github.francescodonnini.query;

import com.influxdb.client.InfluxDBClient;

import java.io.Serializable;

public interface InfluxDbWriterFactory extends Serializable {
    InfluxDbWriterFactory setHost(String host);
    InfluxDbWriterFactory setPort(int port);
    InfluxDbWriterFactory setUsername(String username);
    InfluxDbWriterFactory setPassword(String password);
    InfluxDbWriterFactory setOrg(String org);
    InfluxDbWriterFactory setBucket(String bucket);
    InfluxDbWriterFactory setToken(String token);
    InfluxDBClient create();
}
