package net.thumbtack.sharding;


import java.util.Map;
import java.util.Properties;

public class SqlShard extends Shard {

	public SqlShard(Map<Object, Object> config) {
		super(config);
	}

	public SqlShard(Properties properties) {
		super(properties);
	}
}
