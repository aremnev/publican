package net.thumbtack.sharding.query;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UpdateAllShards extends Query {

	private static final Logger logger = LoggerFactory.getLogger("UpdateAllShards");

	public UpdateAllShards(QueryEngine engine) {
		super(engine);
	}

	@Override
	public <U> U query(QueryClosure<U> closure) {
		U result = null;
		int shardNum = 0;
		try {
			for (shardNum = 0; shardNum < engine.shardsCount(); shardNum++) {
				SqlSession session = engine.openSession(shardNum, closure.getExecutorType());
				try {
					result = closure.call(session);
					session.commit();
				} catch (RuntimeException e) {
					session.rollback();
					throw e;
				} finally {
					session.close();
				}
			}
		} finally {
			// check that all shards was updated
			if (shardNum > 0 && shardNum < engine.shardsCount()) {
				List<Integer> updated = new ArrayList<Integer>(shardNum);
				for (int i = 0; i < shardNum; i++) {
					updated.add(i);
				}
				List<Integer> notUpdated = new ArrayList<Integer>(engine.shardsCount() - shardNum);
				for (int i = shardNum; i < engine.shardsCount(); i++) {
					notUpdated.add(i);
				}
				logger.error("SHARDS OUT OF SYNC. " +
						"Shards {} was updated, shards {} was not. See further logged error.", updated, notUpdated);
			}
		}
		return result;
	}
}
