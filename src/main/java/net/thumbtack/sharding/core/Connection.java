package net.thumbtack.sharding.core;

public interface Connection {

    void open();

    void commit();

    void rollback();

    void close();
}
