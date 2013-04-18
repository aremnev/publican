package net.thumbtack.sharding.core;

import java.util.List;

public class ShardingConfig {

    private List<? extends ShardConfig> shardConfigs;
    private ShardFactory shardFactory;
    private KeyMapper keyMapper;
    private List<QueryConfig> queryConfigs;
    private int workThreads;

    public ShardingConfig() {}

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

    public List<? extends ShardConfig> getShardConfigs() {
        return shardConfigs;
    }

    public void setShardConfigs(List<? extends ShardConfig> shardConfigs) {
        this.shardConfigs = shardConfigs;
    }

    public ShardFactory getShardFactory() {
        return shardFactory;
    }

    public void setShardFactory(ShardFactory shardFactory) {
        this.shardFactory = shardFactory;
    }

    public KeyMapper getKeyMapper() {
        return keyMapper;
    }

    public void setKeyMapper(KeyMapper keyMapper) {
        this.keyMapper = keyMapper;
    }

    public List<QueryConfig> getQueryConfigs() {
        return queryConfigs;
    }

    public void setQueryConfigs(List<QueryConfig> queryConfigs) {
        this.queryConfigs = queryConfigs;
    }

    public int getWorkThreads() {
        return workThreads;
    }

    public void setWorkThreads(int workThreads) {
        this.workThreads = workThreads;
    }
}
