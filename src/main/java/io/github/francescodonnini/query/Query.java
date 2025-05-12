package io.github.francescodonnini.query;

import java.io.Serializable;

public interface Query extends AutoCloseable, Serializable {
    void submit();
}
