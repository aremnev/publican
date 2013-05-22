package net.thumbtack.sharding.core.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

public class HazelcastShardingCluster implements ShardingCluster {

    private static final int DEFAULT_PORT = 5900;
    private static final String EVENT_TOPIC_NAME = "default";
    private static final String LOCK_NAME = "query_lock";
    private static final String IS_LOCKED_VALUE_NAME = "is_locked_value";

    private HazelcastInstance hazelcast;
    private QueryLock queryLock;

    private int port = DEFAULT_PORT;
    private List<String> hosts = new ArrayList<String>();
    private List<Long> queriesToLock = new ArrayList<Long>();

    public HazelcastShardingCluster setPort(int port) {
        this.port = port;
        return this;
    }

    public HazelcastShardingCluster addHost(String host) {
        hosts.add(host);
        return this;
    }

    public HazelcastShardingCluster addQueryToLock(long queryId) {
        queriesToLock.add(queryId);
        return this;
    }

    @Override
    public ShardingCluster start() {
        Config cfg = new Config();
        NetworkConfig networkConfig = cfg.getNetworkConfig();
        networkConfig.setPort(port);
        networkConfig.setPortAutoIncrement(true);
        Join join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig().setEnabled(true);
        for (String host : hosts) {
            tcpIpConfig.addMember(host);
        }

        hazelcast = Hazelcast.newHazelcastInstance(cfg);

        Lock lock = hazelcast.getLock(LOCK_NAME);
        final List<Boolean> isLockedWrapper = hazelcast.getList(IS_LOCKED_VALUE_NAME);
        lock.lock();
        try {
            if (isLockedWrapper.isEmpty()) {
                isLockedWrapper.add(false);
            }
        } finally {
            lock.unlock();
        }
        Value<Boolean> isLockedValue = new Value<Boolean>() {

            @Override
            public Boolean get() {
                return isLockedWrapper.get(0);
            }

            @Override
            public void set(Boolean value) {
                isLockedWrapper.set(0, value);
            }
        };

        queryLock = new QueryLock(lock, isLockedValue, queriesToLock);
        return this;
    }

    @Override
    public void shutdown() {
        hazelcast.getLifecycleService().shutdown();
    }

    @Override
    public QueryLock getQueryLock() {
        return queryLock;
    }

    @Override
    public void addEventProcessor(final EventProcessor processor) {
        final ITopic<Event> topic = hazelcast.getTopic(EVENT_TOPIC_NAME);
        topic.addMessageListener(new MessageListener<Event>() {
            @Override
            public void onMessage(Message<Event> message) {
                processor.onEvent(message.getMessageObject());
            }
        });
        processor.setEventListener(new EventListener() {
            @Override
            public void onEvent(Event event) {
                topic.publish(event);
            }
        });
    }
}
