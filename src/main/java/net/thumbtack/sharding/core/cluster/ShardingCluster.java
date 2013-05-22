package net.thumbtack.sharding.core.cluster;

public interface ShardingCluster {

    ShardingCluster start();

    void shutdown();

    QueryLock getQueryLock();

    void addEventProcessor(EventProcessor processor);
}
