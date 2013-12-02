package net.thumbtack.shardcon.cluster;

import org.apache.commons.lang3.mutable.Mutable;

import java.io.Serializable;
import java.util.concurrent.locks.Lock;

/**
 * Cluster provides distributed functionality.
 */
public interface ShardingCluster extends MessageSender {

    /**
     * Gets distributed lock.
     */
    Lock getLock(String lockName);

    /**
     * Gets distributed value.
     */
    <T extends Serializable> Mutable<T> getMutableValue(String valueName, T defaultValue);

    /**
     * Adds listener of distributed messages.
     */
    void addMessageListener(MessageListener messageListener);

    @Override
    /**
     * Sends distributed message.
     */
    void sendMessage(Serializable message);
}
