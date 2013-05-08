package net.thumbtack.sharding.core;

import fj.F;
import net.thumbtack.sharding.core.query.Query;

import java.util.*;

import static net.thumbtack.helper.Util.*;

/**
 * The builder of {@link Sharding} object.
 */
public class ShardingBuilder {

    private static final int DEFAULT_WORK_THREADS = 10;

    private ModuloKeyMapper keyMapper;
    private int workTreads = DEFAULT_WORK_THREADS;
    private List<Shard> shards = new ArrayList<Shard>();
    private Map<Long, Query> queryMap = new HashMap<Long, Query>();

    /**
     * Sets shards.
     * @param shards The shards.
     * @return The builder.
     */
    public ShardingBuilder setShards(Collection<? extends Shard> shards) {
        this.shards = new ArrayList<Shard>(shards);
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
    public ShardingBuilder setKeyMapper(ModuloKeyMapper keyMapper) {
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
        ShardResolver shardResolver = new ShardResolver(shards, keyMapper);
        return new Sharding(queryMap, shardResolver, workTreads);
    }
}
