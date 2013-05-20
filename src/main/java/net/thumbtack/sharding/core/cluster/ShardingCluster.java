package net.thumbtack.sharding.core.cluster;

public interface ShardingCluster {

    QueryLock getQueryLock();

    void addEventProcessor(EventProcessor processor);
}
