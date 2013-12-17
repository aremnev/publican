package net.thumbtack.shardcon.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.*;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class HazelcastCluster implements ShardingCluster {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastCluster.class);

    private static final int DEFAULT_PORT = 5900;
    private static final String EVENT_TOPIC_NAME = "default";
    private static final String LISTENERS_COUNT_NAME = "listeners_count";

    private HazelcastInstance hazelcast;

    private int port = DEFAULT_PORT;
    private List<String> hosts = new ArrayList<>();

    private IAtomicLong listenersCount;

    public HazelcastCluster setPort(int port) {
        logger.debug("setPort({})", port);
        this.port = port;
        return this;
    }

    public HazelcastCluster addHost(String host) {
        logger.debug("addHost({})", host);
        hosts.add(host);
        return this;
    }

    public HazelcastCluster start() {
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

        listenersCount = hazelcast.getAtomicLong(LISTENERS_COUNT_NAME);
        logger.info(
                "Hazelcast cluster started on port {}, with hosts {}",
                hazelcast.getConfig().getNetworkConfig().getPort(),
                hazelcast.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().getMembers()
        );
        return this;
    }

    public void shutdown() {
        hazelcast.getLifecycleService().shutdown();
        logger.info("Hazelcast cluster shutdown");
    }

    @Override
    public Lock getLock(String lockName) {
        logger.debug("getLock({})", lockName);
        return hazelcast.getLock(lockName);
    }

    @Override
    public <T extends Serializable> Mutable<T> getMutableValue(String valueName, T defaultValue) {
        logger.debug("getMutableValue({}, {})", valueName, defaultValue);
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
    public void addMessageListener(final MessageListener messageListener) {
        logger.debug(("addMessageListener"));
        final ITopic<Serializable> topic = hazelcast.getTopic(EVENT_TOPIC_NAME);
        topic.addMessageListener(new com.hazelcast.core.MessageListener<Serializable>() {
            @Override
            public void onMessage(Message<Serializable> message) {
                messageListener.onMessage(message);
            }
        });
        listenersCount.addAndGet(1);
        messageListener.setMessageDeliveredListener(new MessageListener.MessageDeliveredListener() {
            @Override
            public void onDelivered(Serializable message) {
                IAtomicLong deliveredCount = hazelcast.getAtomicLong(message.getClass().getName());
                deliveredCount.addAndGet(1);
            }
        });
    }

    @Override
    public void waitDelivery(Class<?> messageClass) throws InterruptedException {
        logger.debug("waitDelivery({})", messageClass);
        while (! isDelivered(messageClass)) {
            Thread.sleep(10);
        }
    }

    @Override
    public void sendMessage(Serializable message) {
        logger.debug("sendMessage", message);
        ITopic<Serializable> topic = hazelcast.getTopic(EVENT_TOPIC_NAME);
        topic.publish(message);
    }

    public boolean isDelivered(Class<?> messageClass) {
        IAtomicLong deliveredCount = hazelcast.getAtomicLong(messageClass.getName());
        logger.debug("isDelivered({}), deliveredCount = {}, listenersCount = ", messageClass, deliveredCount.get(), listenersCount.get());
        if (deliveredCount.get() == listenersCount.get()) {
            deliveredCount.set(0);
        }
        return deliveredCount.get() == 0;
    }
}
