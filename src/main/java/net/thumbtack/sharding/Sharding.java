package net.thumbtack.sharding;


import java.util.*;

public class Sharding {

	private Map<Object, Object> config;
	private Map<String, Shard> shards = new HashMap<String, Shard>(0);
	private List<KeyMapper> keyMappers = new ArrayList<KeyMapper>(0);


	public Sharding() {
		this(new HashMap<Object, Object>());
	}

	public Sharding(Map<Object, Object> config) {
		this.config = config;
	}

	public Map<Object, Object> getConfig() {
		return config;
	}

	public void addShard(String id, Shard shard) {
		shards.put(id, shard);
	}

	public Shard getShard(String id) {
		return shards.get(id);
	}

	public Collection<Shard> getShardsList() {
		return shards.values();
	}

	public int getShardCount() {
		return shards.size();
	}

	public void addKeyMapper(KeyMapper keyMapper) {
		keyMappers.add(keyMapper);
	}

}
