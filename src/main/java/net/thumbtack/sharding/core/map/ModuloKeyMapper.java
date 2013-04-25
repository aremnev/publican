package net.thumbtack.sharding.core.map;

/**
 * Maps key to shard by modulo algorithm.
 */
public class ModuloKeyMapper implements KeyMapper {

    private int count;

    /**
     * Constructor.
     * @param count count of shards.
     */
    public ModuloKeyMapper(int count) {
        this.count = count;
    }

    @Override
    public long shard(long key) {
        return key % count;
    }
}