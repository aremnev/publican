package net.thumbtack.sharding.core;

import java.util.List;

public class ShardingFacade {

    private Sharding sharding;

    public ShardingFacade(Sharding sharding) {
        this.sharding = sharding;
    }

    // select from specific shard defined by userId
    public <U> U selectSpec(long id, QueryClosure<U> closure) {
        return sharding.execute(Sharding.SELECT_SPEC_SHARD, id, closure);
    }

    // select from undefined shard
    public <U> U select(QueryClosure<U> closure) {
        return sharding.execute(Sharding.SELECT_SHARD, closure);
    }

    // select from any shard
    public <U> U selectAny(QueryClosure<U> closure) {
        return sharding.execute(Sharding.SELECT_ANY_SHARD, closure);
    }

    // select from all shards with results union
    public <U> List<U> selectAll(QueryClosure<List<U>> closure) {
        return sharding.execute(Sharding.SELECT_ALL_SHARDS, closure);
    }

    // select from all shards with results union
    public Integer selectSumAll(QueryClosure<Integer> closure) {
        return sharding.execute(Sharding.SELECT_ALL_SHARDS_SUM, closure);
    }

    // update on specific shard defined by userId
    public <U> U updateSpec(long id, QueryClosure<U> closure) {
        return sharding.execute(Sharding.UPDATE_SPEC_SHARD, id, closure);
    }

    // update on all shards
    // the result of the method is the last successful closure call result
    public <U> U updateAll(QueryClosure<U> closure) {
        return sharding.execute(Sharding.UPDATE_ALL_SHARDS, closure);
    }
}
