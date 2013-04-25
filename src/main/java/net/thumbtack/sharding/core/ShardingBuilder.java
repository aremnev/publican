package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardingBuilder {

    private ModuloKeyMapper keyMapper;
    private int workTreads = 10;
    private List<Shard> shards = new ArrayList<Shard>();
    private Map<Long, Query> queryMap = new HashMap<Long, Query>();

    public ShardingBuilder addShard(Shard shard) {
        shards.add(shard);
        return this;
    }

    public ShardingBuilder addQuery(long queryId, Query query) {
        queryMap.put(queryId, query);
        return this;
    }

    public ShardingBuilder setKeyMapper(ModuloKeyMapper keyMapper) {
        this.keyMapper = keyMapper;
        return this;
    }

    public ShardingBuilder setWorkTreads(int workTreads) {
        this.workTreads = workTreads;
        return this;
    }

    public Sharding build() {
        if (keyMapper == null) {
            keyMapper = new ModuloKeyMapper(shards.size());
        }
        ShardResolver shardResolver = new ShardResolver(shards, keyMapper);
        return new Sharding(queryMap, shardResolver, workTreads);
    }
}
