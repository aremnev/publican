package net.thumbtack.sharding.vbucket;

import java.util.Map;

public interface ConfigurationApi {

    /**
     * @return Chunk-to-shard map.
     */
    Map<Integer, Integer> getChunkToShardMap();

    /**
     * Moves the chunk to the shard.
     * After this operation the pair (chunk, oldShard)
     * should be in result of getInactiveChunks().
     * @param chunk Chunk to move id.
     * @param shardTo Destination shard id.
     */
    void moveChunk(int chunk, int shardTo);

    /**
     * @param shardId New shard id.
     */
    void addShard(int shardId);

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
