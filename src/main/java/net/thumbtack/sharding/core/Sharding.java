package net.thumbtack.sharding.core;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Sharding {

	private final long INVALID_ID = Long.MIN_VALUE;

	public static final int SELECT_SPEC_SHARD = 1;          // select from specific shard
	public static final int SELECT_SHARD = 2;               // select from undefined shard
	public static final int SELECT_ANY_SHARD = 3;           // select from any shard
	public static final int SELECT_ALL_SHARDS = 4;          // select from all shards with results union to list
	public static final int SELECT_ALL_SHARDS_SUM = 5;      // select from all shards with summation of results to int
	public static final int UPDATE_SPEC_SHARD = 6;          // update on specific shard
	public static final int UPDATE_ALL_SHARDS = 7;          // update on all shards

	private ShardResolver shardResolver;

	private QueryRegistry queryRegistry = new QueryRegistry();
	private Configuration config;

	public Sharding(Configuration config) {
		this.config = config;

		Random random = new Random(config.getSelectAnyRandomSeed());
		queryRegistry.register(SELECT_SPEC_SHARD, true, new SelectSpecShard());
		queryRegistry.register(SELECT_SHARD, true, new SelectShard());
		queryRegistry.register(SELECT_ANY_SHARD, true, new SelectAnyShard(random));
		queryRegistry.register(SELECT_ALL_SHARDS, true, new SelectAllShards());
		queryRegistry.register(SELECT_ALL_SHARDS_SUM, true, new SelectAllShardsSum());
		queryRegistry.register(UPDATE_SPEC_SHARD, true, new SelectSpecShard());
		queryRegistry.register(UPDATE_ALL_SHARDS, true, new SelectAllShards());

		Executor queryExecutor = Executors.newFixedThreadPool(config.getNumberOfWorkerThreads());
		queryRegistry.register(SELECT_SPEC_SHARD, false, new SelectSpecShard());
		queryRegistry.register(SELECT_SHARD, false, new SelectShard());
		queryRegistry.register(SELECT_ANY_SHARD, false, new SelectAnyShard(random));
		queryRegistry.register(SELECT_ALL_SHARDS, false, new SelectAllShardsAsync(queryExecutor));
		queryRegistry.register(SELECT_ALL_SHARDS_SUM, false, new SelectAllShardsSumAsync(queryExecutor));
		queryRegistry.register(UPDATE_SPEC_SHARD, false, new SelectSpecShard());
		queryRegistry.register(UPDATE_ALL_SHARDS, false, new SelectAllShardsAsync(queryExecutor));
	}

	public <V> V execute(int queryType, long id, QueryClosure<V> closure) {
		Query query = queryRegistry.get(queryType);
		List<Connection> connections = resolveShards(queryType, id);
		return query.query(closure, connections);
	}

	public <V> V execute(int queryType, QueryClosure<V> closure) {
		return execute(queryType, INVALID_ID, closure);
	}

	private List<Connection> resolveShards(int queryType, long id) {
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
