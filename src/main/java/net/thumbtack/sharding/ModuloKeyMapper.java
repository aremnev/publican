package net.thumbtack.sharding;


public class ModuloKeyMapper implements KeyMapper {

	private int count;

	public ModuloKeyMapper(Sharding sharding) {
		this.count = sharding.getShardCount();
	}

	@Override
	public long shard(long key) {
		return key % count;
	}

}
