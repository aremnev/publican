package net.thumbtack.sharding.impl.memcached;

import net.thumbtack.sharding.core.ShardConfig;

import java.util.*;

/**
 * The jdbc shard configuration.
 */
public class MemcachedShardConfig extends ShardConfig {

    private static final String SHARD = "shard.";
    private static final String HOST = ".host";
    private static final String PORT = ".port";


    private String host;
    private int port;

    /**
     * Builds list of jdbc shard configurations from properties.
     * @param props The properties.
     * @return The list of jdbc shard configurations.
     */
    public static List<MemcachedShardConfig> fromProperties(Properties props) {
        Set<Long> shardIds = new HashSet<Long>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(SHARD)) {
                long id = Long.parseLong(name.split("\\.")[1]);
                shardIds.add(id);
            }
        }
        List<MemcachedShardConfig> result = new ArrayList<MemcachedShardConfig>(shardIds.size());
        for (long shardId : shardIds) {
            MemcachedShardConfig config = new MemcachedShardConfig();
            config.setId(shardId);
            config.setHost(props.getProperty(SHARD + shardId + HOST));
            config.setPort(Integer.parseInt(props.getProperty(SHARD + shardId + PORT)));

            result.add(config);
        }
        return result;
    }

    /**
     * Default constructor.
     */
    public MemcachedShardConfig() {
        super();
    }

    /**
     * Constructor.
     * @param id The shard id.
     * @param host The url to memcashed.
     * @param port The port.
     */
    public MemcachedShardConfig(long id, String host, int port) {
        super(id);
        this.host = host;
        this.port = port;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
