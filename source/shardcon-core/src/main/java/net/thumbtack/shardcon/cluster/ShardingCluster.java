package net.thumbtack.shardcon.cluster;

import org.apache.commons.lang3.mutable.Mutable;

import java.io.Serializable;
import java.util.concurrent.locks.Lock;

public interface ShardingCluster extends MessageSender {

    Lock getLock(String lockName);

    <T> Mutable<T> getMutableValue(String valueName, T defaultValue);

    void addMessageListener(MessageListener messageListener);

    @Override
    void sendMessage(Serializable message);
}
