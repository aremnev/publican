package net.thumbtack.sharding.core.cluster;

public interface ShardingCluster {

    ShardingCluster start();

    QueryLock getQueryLock();

    void addEventProcessor(EventProcessor processor);
}
