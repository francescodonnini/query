package io.github.francescodonnini.query;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;

public class InfluxDbWriterFactoryImpl implements InfluxDbWriterFactory {
    private String host;
    private int port;
    private String username;
    private String password;
    private String org;
    private String bucket;
    private String token;


    @Override
    public InfluxDbWriterFactory setHost(String host) {
        this.host = host;
        return this;
    }

    @Override
    public InfluxDbWriterFactory setPort(int port) {
        this.port = port;
        return this;
    }

    @Override
    public InfluxDbWriterFactory setUsername(String username) {
        this.username = username;
        return this;
    }

    @Override
    public InfluxDbWriterFactory setPassword(String password) {
        this.password = password;
        return this;
    }

    @Override
    public InfluxDbWriterFactory setOrg(String org) {
        this.org = org;
        return this;
    }

    @Override
    public InfluxDbWriterFactory setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    @Override
    public InfluxDbWriterFactory setToken(String token) {
        this.token = token;
        return this;
    }

    @Override
    public InfluxDBClient create() {
        var opts = InfluxDBClientOptions.builder()
                .url("http://" + host + ":" + port)
                .authenticate(username, password.toCharArray())
                .authenticateToken(token.toCharArray())
                .org(org)
                .bucket(bucket)
                .build();
        return InfluxDBClientFactory.create(opts);
    }
}
