package net.thumbtack.shardcon.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.*;
import org.apache.commons.lang3.mutable.Mutable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

public class HazelcastCluster implements ShardingCluster {

    private static final int DEFAULT_PORT = 5900;
    private static final String EVENT_TOPIC_NAME = "default";

    private HazelcastInstance hazelcast;

    private int port = DEFAULT_PORT;
    private List<String> hosts = new ArrayList<>();

    public HazelcastCluster setPort(int port) {
        this.port = port;
        return this;
    }

    public HazelcastCluster addHost(String host) {
        hosts.add(host);
        return this;
    }

    @Override
    public ShardingCluster start() {
        Config cfg = new Config();
        NetworkConfig networkConfig = cfg.getNetworkConfig();
        networkConfig.setPort(port);
        networkConfig.setPortAutoIncrement(true);
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig().setEnabled(true);
        for (String host : hosts) {
            tcpIpConfig.addMember(host);
        }

        hazelcast = Hazelcast.newHazelcastInstance(cfg);
        return this;
    }

    @Override
    public void shutdown() {
        hazelcast.getLifecycleService().shutdown();
    }

    @Override
    public Lock getLock(String lockName) {
        return hazelcast.getLock(lockName);
    }

    @Override
    public <T> Mutable<T> getMutableValue(String valueName, T defaultValue) {
        final List<T> valueWrapper = hazelcast.getList(valueName);
        Lock lock = hazelcast.getLock(valueName);
        lock.lock();
        try {
            if (valueWrapper.isEmpty()) {
                valueWrapper.add(defaultValue);
            }
        } finally {
            lock.unlock();
        }
        return new Mutable<T>() {

            @Override
            public T getValue() {
                return valueWrapper.get(0);
            }

            @Override
            public void setValue(T value) {
                valueWrapper.set(0, value);
            }
        };
    }

    @Override
    public void addMessageOriginator(MessageOriginator messageOriginator) {
        final ITopic<Serializable> topic = hazelcast.getTopic(EVENT_TOPIC_NAME);
        messageOriginator.setMessageSender(new MessageSender() {
            @Override
            public void sendMessage(Serializable message) {
                topic.publish(message);
            }
        });
    }

    @Override
    public void addMessageListener(final MessageListener messageListener) {
        final ITopic<Serializable> topic = hazelcast.getTopic(EVENT_TOPIC_NAME);
        topic.addMessageListener(new com.hazelcast.core.MessageListener<Serializable>() {
            @Override
            public void onMessage(Message<Serializable> message) {
                messageListener.onMessage(message);
            }
        });
    }
}
