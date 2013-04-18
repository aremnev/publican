package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.Connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolve the shards by id.
 */
class ShardResolver {

    private Map<Long, Shard> shards = new HashMap<Long, Shard>(0);
    private KeyMapper keyMapper;

    /**
     * Constructor.
     * @param shards The shards.
     * @param keyMapper The key mapper.
     */
    public ShardResolver(Iterable<Shard> shards, KeyMapper keyMapper) {
        for (Shard shard : shards) {
            this.shards.put(shard.getId(), shard);
        }
        this.keyMapper = keyMapper;
    }

    /**
     * Resolve the shard by id.
     * @param id The id.
     * @return The shard.
     */
    public Connection resolveId(long id) {
        long shardId = keyMapper.shard(id);
        Shard shard = shards.get(shardId);
        return createConnection(shard);
    }

    /**
     * Returns the all shards.
     * @return The all shards.
     */
    public List<Connection> resolveAll() {
        List<Connection> result = new ArrayList<Connection>(shards.size());
        for (Shard shard : shards.values()) {
            result.add(createConnection(shard));
        }
        return result;
    }

    private Connection createConnection(Shard shard) {
        return shard.getConnection();
    }
}
