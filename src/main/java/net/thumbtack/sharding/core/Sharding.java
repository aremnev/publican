package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.*;

import java.util.*;

/**
 * The main entry point into the library. It provides the interface to execution of queries.
 */
public class Sharding {

    private static final long INVALID_ID = Long.MIN_VALUE;

    private ShardResolver shardResolver;

    private QueryRegistry queryRegistry;

    /**
     * Constructor.
     * @param config The general config.
     */
    public Sharding(ShardingConfig config) {
        List<? extends ShardConfig> shardConfigs = config.getShardConfigs();
        ShardFactory shardFactory = config.getShardFactory();
        List<Shard> shards = new ArrayList<Shard>(config.getShardConfigs().size());
        for (ShardConfig shardConfig : shardConfigs) {
            shards.add(shardFactory.createShard(shardConfig));
        }
        shardResolver = new ShardResolver(shards, config.getKeyMapper());
        queryRegistry = new QueryRegistry(config);
    }

    /**
     * Executes the query on the shard which is resolved by id.
     * @param queryId The query id.
     * @param id The id to resolve shard.
     * @param closure The object that encapsulates the real action.
     * @param <V> The type of query result.
     * @return The result of query execution.
     */
    public <V> V execute(long queryId, long id, QueryClosure<V> closure) {
        Query query = queryRegistry.get(queryId);
        List<Connection> connections = resolveShards(id);
        return query.query(closure, connections);
    }

    /**
     * Executes the query on all shards or undefined shard.
     * @param queryId The query id.
     * @param closure The object that encapsulates the real action.
     * @param <V> The type of query result.
     * @return The result of query execution.
     */
    public <V> V execute(long queryId, QueryClosure<V> closure) {
        return execute(queryId, INVALID_ID, closure);
    }

    private List<Connection> resolveShards(long id) {
        if (id == INVALID_ID) {
            return shardResolver.resolveAll();
        } else {
            Connection connection = shardResolver.resolveId(id);
            List<Connection> list = new ArrayList<Connection>(1);
            list.add(connection);
            return list;
        }
    }
}
