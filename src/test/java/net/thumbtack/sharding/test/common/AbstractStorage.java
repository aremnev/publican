package net.thumbtack.sharding.test.common;

import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.core.query.Query;
import net.thumbtack.sharding.core.query.SelectAllShards;
import net.thumbtack.sharding.core.query.SelectAllShardsAsync;
import net.thumbtack.sharding.core.query.SelectAllShardsSum;
import net.thumbtack.sharding.core.query.SelectAllShardsSumAsync;
import net.thumbtack.sharding.core.query.SelectAnyShard;
import net.thumbtack.sharding.core.query.SelectShard;
import net.thumbtack.sharding.core.query.SelectShardAsync;
import net.thumbtack.sharding.core.query.SelectSpecShard;
import net.thumbtack.sharding.core.query.UpdateAllShards;
import net.thumbtack.sharding.core.query.UpdateAllShardsAsync;
import net.thumbtack.sharding.core.query.UpdateSpecShard;

import java.util.HashMap;
import java.util.Map;

import static net.thumbtack.sharding.ShardingFacade.*;
import static net.thumbtack.sharding.ShardingFacade.UPDATE_ALL_SHARDS;


abstract public class AbstractStorage implements Storage {
    protected Map<Long, Query> getQueryMap(boolean isSync) {
        return isSync ?
                new HashMap<Long, Query>() {
                    {
                        put(SELECT_SPEC_SHARD, new SelectSpecShard());
                        put(SELECT_SHARD, new SelectShard());
                        put(SELECT_ANY_SHARD, new SelectAnyShard());
                        put(SELECT_ALL_SHARDS, new SelectAllShards());
                        put(SELECT_ALL_SHARDS_SUM, new SelectAllShardsSum());
                        put(UPDATE_SPEC_SHARD, new UpdateSpecShard());
                        put(UPDATE_ALL_SHARDS, new UpdateAllShards());
                    }
                } :
                new HashMap<Long, Query>() {
                    {
                        put(SELECT_SPEC_SHARD, new SelectSpecShard());
                        put(SELECT_SHARD, new SelectShardAsync());
                        put(SELECT_ANY_SHARD, new SelectAnyShard());
                        put(SELECT_ALL_SHARDS, new SelectAllShardsAsync());
                        put(SELECT_ALL_SHARDS_SUM, new SelectAllShardsSumAsync());
                        put(UPDATE_SPEC_SHARD, new UpdateSpecShard());
                        put(UPDATE_ALL_SHARDS, new UpdateAllShardsAsync());
                    }
                };
    }
}
