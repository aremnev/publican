package net.thumbtack.sharding.core;

import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.cluster.*;
import net.thumbtack.sharding.core.cluster.EventListener;
import net.thumbtack.sharding.core.query.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The main entry point into the library. It provides the interface to execution of queries.
 */
public class Sharding implements EventProcessor {

    private static final long INVALID_ID = Long.MIN_VALUE;

    private Map<Integer, Shard> shards = new ConcurrentHashMap<Integer, Shard>();
    private KeyMapper keyMapper;
    private QueryLock queryLock;

    private Map<Long, Query> queryRegistry;
    private EventListener eventListener;

    /**
     * Constructor.
     * @param queryRegistry The query map.
     * @param shards The shards.
     * @param keyMapper The key mapper.
     * @param workThreads The number of work threads.
     */
    public Sharding(Map<Long, Query> queryRegistry, Iterable<Shard> shards, KeyMapper keyMapper, int workThreads) {
        this.queryRegistry = queryRegistry;
        for (Shard shard : shards) {
            this.shards.put(shard.getId(), shard);
        }
        this.keyMapper = keyMapper;
        ExecutorService executor = Executors.newFixedThreadPool(workThreads, new NamedThreadFactory("query"));
        for (Query query : queryRegistry.values()) {
            if (query instanceof QueryAsync) {
                ((QueryAsync) query).setExecutor(executor);
            }
        }
    }

    public void addShard(Shard shard) {
        if (! shards.containsKey(shard.getId())) {
            shards.put(shard.getId(), shard);
            if (eventListener != null) {
                eventListener.onEvent(new ShardAddedEvent(shard));
            }
        }
    }

    /**
     * Sets the query lock.
     * @param queryLock The query lock.
     */
    public void setQueryLock(QueryLock queryLock) {
        this.queryLock = queryLock;
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
        if (queryLock != null) {
            queryLock.await(queryId);
        }
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
        if (queryLock != null) {
            queryLock.await(queryId);
        }
        Query query = queryRegistry.get(queryId);
        List<Connection> connections = resolveIds(ids);
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
            return resolveAll();
        } else {
            Connection connection = resolveId(id);
            List<Connection> list = new ArrayList<Connection>(1);
            list.add(connection);
            return list;
        }
    }

    /**
     * Resolve the shard by id.
     * @param id The id.
     * @return The shard.
     */
    public Connection resolveId(long id) {
        int shardId = keyMapper.shard(id);
        Shard shard = shards.get(shardId);
        return shard.getConnection();
    }

    /**
     * Resolve the shards by ids.
     * @param ids The list of id.
     * @return The shards.
     */
    public List<Connection> resolveIds(List<Long> ids) {
        Map<Integer, List<Long>> shardIds = new HashMap<Integer, List<Long>>();
        for (long id : ids) {
            int shardId = keyMapper.shard(id);
            List<Long> cargo = shardIds.get(shardId);
            if (cargo == null) {
                cargo = new ArrayList<Long>();
                shardIds.put(shardId, cargo);
            }
            cargo.add(id);
        }
        List<Connection> connections = new ArrayList<Connection>(shardIds.size());
        for (int shardId : shardIds.keySet()) {
            Shard shard = shards.get(shardId);
            Connection connection = shard.getConnection();
            connection.setCargo(shardIds.get(shardId));
            connections.add(connection);
        }
        return connections;
    }

    /**
     * Returns the all shards.
     * @return The all shards.
     */
    public List<Connection> resolveAll() {
        List<Connection> result = new ArrayList<Connection>(shards.size());
        for (Shard shard : shards.values()) {
            result.add(shard.getConnection());
        }
        return result;
    }

    @Override
    public void onEvent(Event event) {
        if (event.getId() == ShardAddedEvent.ID) {
            addShard(((ShardAddedEvent) event).getEventObject());
        }
    }

    @Override
    public void setEventListener(EventListener listener) {
        eventListener = listener;
    }
}
