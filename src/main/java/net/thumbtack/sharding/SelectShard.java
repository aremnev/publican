package net.thumbtack.sharding;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.Collection;

public class SelectShard extends Query {

	public SelectShard(QueryEngine engine) {
		super(engine);
	}

	@Override
	public <U> U query(QueryClosure<U> closure) {
		U result = null;
		for (int shardKey : engine.shardsKeys()) {
			SqlSession session = engine.openSession(shardKey, closure.getExecutorType());
			try {
				result = closure.call(session);
			} finally {
				session.close();
			}
			if (result != null) {
				if (result instanceof Collection<?>) {
					if (((Collection<?>) result).size() > 0) {
						return result;
					}
				} else {
					return result;
				}
			}
		}

		return result;
	}
}
