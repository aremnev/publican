package net.thumbtack.sharding.core;

public class ModuloKeyMapper implements KeyMapper {

    private int count;

    public ModuloKeyMapper(int count) {
        this.count = count;
    }

    @Override
    public long shard(long key) {
        return key % count;
    }
}