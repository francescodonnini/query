package io.github.francescodonnini.conf;

import java.util.Properties;

public class ConfImpl implements Conf {
    private final Properties properties;

    public ConfImpl(Properties properties) {
        this.properties = properties;
    }

    @Override
    public int getInt(String name) {
        return Integer.parseInt(properties.getProperty(name));
    }

    @Override
    public String getString(String name) {
        return properties.getProperty(name);
    }
}
