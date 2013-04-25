package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.map.KeyMapper;
import net.thumbtack.sharding.core.query.Connection;

import java.util.*;

/**
 * Resolve the shards by id.
 */
class ShardResolver {

    private Map<Integer, Shard> shards = new HashMap<Integer, Shard>(0);
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
}
