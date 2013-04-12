package net.thumbtack.sharding.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UpdateAllShards extends Query {

	private static final Logger logger = LoggerFactory.getLogger("UpdateAllShards");

	@Override
	public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
		U result = null;
		int shardNum = 0;
		try {
			for (shardNum = 0; shardNum < shards.size(); shardNum++) {
				Connection connection = shards.get(shardNum);
				connection.open();
				try {
					result = closure.call(connection);
					connection.commit();
				} catch (RuntimeException e) {
					connection.rollback();
					throw e;
				} finally {
					connection.close();
				}
			}
		} finally {
			// check that all shards was updated
			if (shardNum > 0 && shardNum < shards.size()) {
				List<Integer> updated = new ArrayList<Integer>(shardNum);
				for (int i = 0; i < shardNum; i++) {
					updated.add(i);
				}
				List<Integer> notUpdated = new ArrayList<Integer>(shards.size() - shardNum);
				for (int i = shardNum; i < shards.size(); i++) {
					notUpdated.add(i);
				}
				logger.error("SHARDS OUT OF SYNC. " +
						"Shards {} was updated, shards {} was not. See further logged error.", updated, notUpdated);
			}
		}
		return result;
	}
}
