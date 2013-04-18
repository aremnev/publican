package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.ShardConfig;
import net.thumbtack.sharding.core.ShardFactory;

public class JdbcShardFactory implements ShardFactory {
    @Override
    public Shard createShard(ShardConfig config) {
        JdbcShardConfig jdbcConfig = (JdbcShardConfig) config;
        return new JdbcShard(jdbcConfig);
    }
}
