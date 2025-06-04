package io.github.francescodonnini.conf;

public interface Conf {
    boolean getBoolean(String name);
    int getInt(String name);
    String getString(String name);
}