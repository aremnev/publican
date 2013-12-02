package net.thumbtack.shardcon.cluster;

import org.apache.commons.lang3.mutable.Mutable;

import java.util.concurrent.locks.Lock;

public interface ShardingCluster {

    ShardingCluster start();

    void shutdown();

    Lock getLock(String lockName);

    <T> Mutable<T> getMutableValue(String valueName, T defaultValue);

    void addEventProcessor(EventProcessor processor);
}
