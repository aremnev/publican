package net.thumbtack.sharding.core;

import java.util.List;

/**
 * The general library configuration.
 */
public class ShardingConfig {

    private List<? extends ShardConfig> shardConfigs;
    private ShardFactory shardFactory;
    private KeyMapper keyMapper;
    private List<QueryConfig> queryConfigs;
    private int workThreads;

    /**
     * Default constructor.
     */
    public ShardingConfig() {}

    /**
     * Constructor.
     * @param shardConfigs The list of shard configurations.
     * @param shardFactory The shards factory.
     * @param keyMapper The way to resolve shards.
     * @param queryConfigs The list of query configurations.
     * @param workThreads The number of work threads for execution asynchronous queries.
     */
    public ShardingConfig(
            List<ShardConfig> shardConfigs,
            ShardFactory shardFactory,
            KeyMapper keyMapper,
            List<QueryConfig> queryConfigs,
            int workThreads
    ) {
        this.shardConfigs = shardConfigs;
        this.shardFactory = shardFactory;
        this.keyMapper = keyMapper;
        this.queryConfigs = queryConfigs;
        this.workThreads = workThreads;
    }

    /**
     * Gets the list of shard configurations.
     * @return The list of shard configurations.
     */
    public List<? extends ShardConfig> getShardConfigs() {
        return shardConfigs;
    }

    /**
     * Sets the list of shard configurations.
     * @param shardConfigs The list of shard configurations.
     */
    public void setShardConfigs(List<? extends ShardConfig> shardConfigs) {
        this.shardConfigs = shardConfigs;
    }

    /**
     * Gets the shards factory.
     * @return The shards factory.
     */
    public ShardFactory getShardFactory() {
        return shardFactory;
    }

    /**
     * Sets the shards factory.
     * @param shardFactory The shards factory.
     */
    public void setShardFactory(ShardFactory shardFactory) {
        this.shardFactory = shardFactory;
    }

    /**
     * Gets the key mapper.
     * @return The key mapper.
     */
    public KeyMapper getKeyMapper() {
        return keyMapper;
    }

    /**
     * Sets the key mapper.
     * @param keyMapper The key mapper.
     */
    public void setKeyMapper(KeyMapper keyMapper) {
        this.keyMapper = keyMapper;
    }

    /**
     * Gets the list of query configurations.
     * @return The list of query configurations.
     */
    public List<QueryConfig> getQueryConfigs() {
        return queryConfigs;
    }

    /**
     * Sets the list of query configurations.
     * @param queryConfigs The list of query configurations.
     */
    public void setQueryConfigs(List<QueryConfig> queryConfigs) {
        this.queryConfigs = queryConfigs;
    }

    /**
     * Gets the number of work threads.
     * @return The number of work threads.
     */
    public int getWorkThreads() {
        return workThreads;
    }

    /**
     * Sets the number of work threads.
     * @param workThreads The number of work threads.
     */
    public void setWorkThreads(int workThreads) {
        this.workThreads = workThreads;
    }
}
