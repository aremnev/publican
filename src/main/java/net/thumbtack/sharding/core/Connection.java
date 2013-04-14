package net.thumbtack.sharding.core;

public interface Connection {

    void open() throws Exception;

    void commit() throws Exception;

    void rollback() throws Exception;

    void close() throws Exception;
}
