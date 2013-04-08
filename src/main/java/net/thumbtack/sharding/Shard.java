package net.thumbtack.sharding;

import java.util.Map;

public class Shard {

	Map<Object, Object> config;

	public Shard(Map<Object, Object> config) {
		this.config = config;
	}

}
