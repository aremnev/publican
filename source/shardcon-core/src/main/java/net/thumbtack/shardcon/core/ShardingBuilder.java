package net.thumbtack.shardcon.core;

import fj.F;
import net.thumbtack.shardcon.cluster.MessageSender;
import net.thumbtack.shardcon.cluster.ShardingCluster;
import net.thumbtack.shardcon.core.query.Query;

import java.util.*;

import static net.thumbtack.helper.Util.*;

/**
 * The builder of {@link Sharding} object.
 */
public class ShardingBuilder {

    public static final String QUERY_LOCK_NAME = "queryLock";
    public static final String IS_LOCKED_VALUE_NAME = "isLocked";

    private static final int DEFAULT_WORK_THREADS = 10;

    private KeyMapper keyMapper;
    private int workTreads = DEFAULT_WORK_THREADS;
    private List<Shard> shards = new ArrayList<>(0);
    private Map<Long, Query> queryMap = new HashMap<>(0);
    private List<Long> queriesToLock = new ArrayList<>(0);
    private ShardingCluster shardingCluster;

    /**
     * Sets shards.
     * @param shards The shards.
     * @return The builder.
     */
    public ShardingBuilder setShards(Collection<? extends Shard> shards) {
        this.shards = new ArrayList<>(shards);
        return this;
    }

    /**
     * Adds one more query.
     * @param queryId The query id.
     * @param query The query.
     * @return The builder.
     */
    public ShardingBuilder addQuery(long queryId, Query query) {
        queryMap.put(queryId, query);
        return this;
    }

    /**
     * Sets key mapper.
     * @param keyMapper The key mapper.
     * @return The builder.
     */
    public ShardingBuilder setKeyMapper(KeyMapper keyMapper) {
        this.keyMapper = keyMapper;
        return this;
    }

    /**
     * Sets the number of work threads.
     * @param workTreads The number of work threads.
     * @return The builder.
     */
    public ShardingBuilder setWorkTreads(int workTreads) {
        this.workTreads = workTreads;
        return this;
    }

    public ShardingBuilder setQueriesToLock(List<Long> queriesToLock) {
        this.queriesToLock = new ArrayList<>(queriesToLock);
        return this;
    }

    public ShardingBuilder setShardingCluster(ShardingCluster shardingCluster) {
        this.shardingCluster = shardingCluster;
        return this;
    }

    /**
     * Builds the Sharding object.
     * @return The Sharding object.
     */
    public Sharding build() {
        if (keyMapper == null) {
            keyMapper = new ModuloKeyMapper(map(shards, new F<Shard, Integer>() {
                @Override
                public Integer f(Shard shard) {
                    return shard.getId();
                }
            }));
        }

        QueryLock queryLock = null;
        if (shardingCluster != null) {
            queryLock = new QueryLock(
                    shardingCluster.getLock(QUERY_LOCK_NAME),
                    shardingCluster.getMutableValue(IS_LOCKED_VALUE_NAME, false),
                    queriesToLock
            );
        }
        Sharding sharding = new Sharding(queryMap, shards, keyMapper, workTreads, queryLock);
        if (shardingCluster != null) {
            shardingCluster.addMessageListener(sharding);
        }
        return sharding;
    }
}
