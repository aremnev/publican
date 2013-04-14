package net.thumbtack.sharding.core;

public interface KeyMapper {

    public long shard(long key);
}
