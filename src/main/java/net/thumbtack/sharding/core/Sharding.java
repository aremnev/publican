package net.thumbtack.sharding.core;

import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.query.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The main entry point into the library. It provides the interface to execution of queries.
 */
public class Sharding {

    private static final long INVALID_ID = Long.MIN_VALUE;

    private ShardResolver shardResolver;

    private Map<Long, Query> queryRegistry;

    /**
     * Constructor.
     * @param queryRegistry The query map.
     * @param shardResolver The shard resolver.
     * @param workThreads The number of work threads.
     */
    public Sharding(Map<Long, Query> queryRegistry, ShardResolver shardResolver, int workThreads) {
        this.queryRegistry = queryRegistry;
        this.shardResolver = shardResolver;
        ExecutorService executor = Executors.newFixedThreadPool(workThreads, new NamedThreadFactory("query"));
        for (Query query : queryRegistry.values()) {
            if (query instanceof QueryAsync) {
                ((QueryAsync) query).setExecutor(executor);
            }
        }
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
     * Executes the query on the several shards which is resolved by id.
     * @param queryId The query id.
     * @param ids The list of ids to resolve shard.
     * @param closure The object that encapsulates the real action.
     * @param <V> The type of query result.
     * @return The result of query execution.
     */
    public <V> V execute(long queryId, List<Long> ids, QueryClosure<V> closure) {
        Query query = queryRegistry.get(queryId);
        List<Connection> connections = shardResolver.resolveIds(ids);
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
