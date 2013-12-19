package net.thumbtack.shardcon.chunk;

import net.thumbtack.shardcon.core.Shard;

import java.util.List;
import java.util.Map;

public interface ConfigurationBroker {

    /**
     * @return Actual configuration version.
     */
    long getVersion();

    /**
     * @return Chunk-to-shard map.
     */
    Map<Integer, Integer> getChunkToShardMap();

    /**
     * Moves the chunk to the shard.
     * After this operation the pair (chunk, oldShard)
     * should be in result of getInactiveChunks().
     * @param version Current client version.
     * @param chunk Chunk to move id.
     * @param shardTo Destination shard id.
     * @return New configuration version.
     */
    long moveChunk(long version, int chunk, int shardTo);

    /**
     * @param version Current client version.
     * @param shard New shard.
     * @return New configuration version.
     */
    long addShard(long version, Shard shard);

    /**
     * @param version Current client version.
     * @param shardId Shard id.
     * @return New configuration version.
     */
    long removeShard(long version, int shardId);

    /**
     * @return The all shards.
     */
    List<Shard> getShards();

    /**
     * @return Chunk-to-shard map of inactive chunks.
     */
    Map<Integer, Integer> getInactiveChunks();

    /**
     * Removes inactive chunk from storage.
     * After this operation the pair (chunk, shardFrom)
     * shouldn't be in result of getInactiveChunks().
     * @param chunk Chunk to remove id
     * @param shardFrom Shard to remove from id.
     */
    void removeInactiveChunk(int chunk, int shardFrom);
}
