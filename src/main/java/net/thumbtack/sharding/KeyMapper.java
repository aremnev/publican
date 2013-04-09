package net.thumbtack.sharding;

public interface KeyMapper {

	public long shard(long key);
}
