package io.github.francescodonnini.query;

public interface Query extends AutoCloseable {
    void submit();
}
