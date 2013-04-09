package net.thumbtack.sharding;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

public class SelectAllShardsSum extends Query {

	public SelectAllShardsSum(QueryEngine engine) {
		super(engine);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <U> U query(QueryClosure<U> closure) {
		Integer result = 0;
		for (int shardKey : engine.shardsKeys()) {
			SqlSession session = engine.openSession(shardKey, closure.getExecutorType());
			try {
				Integer res = (Integer) closure.call(session);
				if (res != null) {
					result += res;
				}
			} finally {
				session.close();
			}
		}
		return (U) result;
	}
}
