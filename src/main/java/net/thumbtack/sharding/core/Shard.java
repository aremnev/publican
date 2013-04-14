package net.thumbtack.sharding.core;

public interface Shard {

    long getId();

    Connection getConnection() throws Exception;
}
