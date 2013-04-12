package net.thumbtack.sharding.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardResolver {

	private Map<Long, Shard> shards = new HashMap<Long, Shard>(0);
	private KeyMapper keyMapper;

	public Connection resolveId(long id) {
		long shardId = keyMapper.shard(id);
		Shard shard = shards.get(shardId);
		return createConnection(shard);
	}

	public List<Connection> resolveAll() {
		List<Connection> result = new ArrayList<Connection>(shards.size());
		for (Shard shard : shards.values()) {
			result.add(createConnection(shard));
		}
		return result;
	}

	private Connection createConnection(Shard shard) {
		return shard.getConnection();
	}
}
