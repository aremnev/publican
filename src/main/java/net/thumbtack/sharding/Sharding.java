package net.thumbtack.sharding;

import net.thumbtack.sharding.Query;
import net.thumbtack.sharding.QueryClosure;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
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

	private Map<String, Shard> shards = new HashMap<String, Shard>(0);
	private List<KeyMapper> keyMappers = new ArrayList<KeyMapper>(0);

	private QueryRegistry queryRegistry = new QueryRegistry();

	public Sharding() {
		long randomSeed = 4; // TODO get random seed from config
		Random random = new Random(randomSeed);
		queryRegistry.register(SELECT_SPEC_SHARD, true, new SelectSpecShard());
		queryRegistry.register(SELECT_SHARD, true, new SelectShard());
		queryRegistry.register(SELECT_ANY_SHARD, true, new SelectAnyShard(random));
		queryRegistry.register(SELECT_ALL_SHARDS, true, new SelectAllShards());
		queryRegistry.register(SELECT_ALL_SHARDS_SUM, true, new SelectAllShardsSum());
		queryRegistry.register(UPDATE_SPEC_SHARD, true, new SelectSpecShard());
		queryRegistry.register(UPDATE_ALL_SHARDS, true, new SelectAllShards());

		int nThreads = 20; // TODO get nThreads from config
		Executor queryExecutor = Executors.newFixedThreadPool(nThreads);
		queryRegistry.register(SELECT_SPEC_SHARD, false, new SelectSpecShard());
		queryRegistry.register(SELECT_SHARD, false, new SelectShard());
		queryRegistry.register(SELECT_ANY_SHARD, false, new SelectAnyShard(random));
		queryRegistry.register(SELECT_ALL_SHARDS, false, new SelectAllShardsAsync(queryExecutor));
		queryRegistry.register(SELECT_ALL_SHARDS_SUM, false, new SelectAllShardsSumAsync(queryExecutor));
		queryRegistry.register(UPDATE_SPEC_SHARD, false, new SelectSpecShard());
		queryRegistry.register(UPDATE_ALL_SHARDS, false, new SelectAllShardsAsync(queryExecutor));
	}

	protected <V> V execute(int queryType, long id, QueryClosure<V> closure) {
		Query query = queryRegistry.get(queryType);
		List<Connection> connections = new ArrayList<Connection>(); // TODO resolve shards and get connections
		return query.query(closure, connections);
	}

	protected <V> V execute(int queryType, QueryClosure<V> closure) {
		return execute(queryType, INVALID_ID, closure);
	}
}
