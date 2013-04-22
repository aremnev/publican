package net.thumbtack.sharding;

import net.thumbtack.sharding.core.Sharding;
import net.thumbtack.sharding.core.query.QueryClosure;

import java.util.List;

public class ShardingFacade {

    public static final long SELECT_SPEC_SHARD = 1001;          // select from specific shard
    public static final long SELECT_SHARD = 1002;               // select from undefined shard
    public static final long SELECT_ANY_SHARD = 1003;           // select from any shard
    public static final long SELECT_ALL_SHARDS = 1004;          // select from all shards with union of results to list
    public static final long SELECT_ALL_SHARDS_SUM = 1005;      // select from all shards with summation of results to int
    public static final long UPDATE_SPEC_SHARD = 1006;          // update on specific shard
    public static final long UPDATE_ALL_SHARDS = 1007;          // update on all shards

    private Sharding sharding;

    public ShardingFacade(Sharding sharding) {
        this.sharding = sharding;
    }

    // select from specific shard defined by userId
    public <U> U selectSpec(long id, QueryClosure<U> closure) {
        return sharding.execute(SELECT_SPEC_SHARD, id, closure);
    }

    // select from undefined shard
    public <U> U select(QueryClosure<U> closure) {
        return sharding.execute(SELECT_SHARD, closure);
    }

    // select from any shard
    public <U> U selectAny(QueryClosure<U> closure) {
        return sharding.execute(SELECT_ANY_SHARD, closure);
    }

    // select from all shards with results union
    public <U> List<U> selectAll(QueryClosure<List<U>> closure) {
        return sharding.execute(SELECT_ALL_SHARDS, closure);
    }

    // select from all shards with results union and with resolving shards by several ids
    public <U> List<U> selectAll(List<Long> ids, QueryClosure<List<U>> closure) {
        return sharding.execute(SELECT_ALL_SHARDS, ids, closure);
    }

    // select from all shards with results union
    public Integer selectSumAll(QueryClosure<Integer> closure) {
        return sharding.execute(SELECT_ALL_SHARDS_SUM, closure);
    }

    // update on specific shard defined by userId
    public <U> U updateSpec(long id, QueryClosure<U> closure) {
        return sharding.execute(UPDATE_SPEC_SHARD, id, closure);
    }

    // update on all shards
    // the result of the method is the last successful closure call result
    public <U> U updateAll(QueryClosure<U> closure) {
        return sharding.execute(UPDATE_ALL_SHARDS, closure);
    }

    // update on all shards resolver by ids
    // the result of the method is the last successful closure call result
    public <U> U updateAll(List<Long> ids, QueryClosure<U> closure) {
        return sharding.execute(UPDATE_ALL_SHARDS, ids, closure);
    }
}
