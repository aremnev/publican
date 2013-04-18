package net.thumbtack.sharding.core;

public interface Configuration {

    QueryRegistry getQueryRegistry();

    Iterable<Shard> getShards();

    KeyMapper getKeyMapper();
}
