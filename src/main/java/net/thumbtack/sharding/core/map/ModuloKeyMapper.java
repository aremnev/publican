package net.thumbtack.sharding.core.map;

import java.util.List;

/**
 * Maps key to shard by modulo algorithm.
 */
public class ModuloKeyMapper implements KeyMapper {

    private List<Integer> shardsIds;

    /**
     * Constructor.
     * @param shardsIds The shards.
     */
    public ModuloKeyMapper(List<Integer> shardsIds) {
        this.shardsIds = shardsIds;
    }

    @Override
    public int shard(long key) {
        return shardsIds.get((int) key % shardsIds.size());
    }
}