package net.thumbtack.sharding.query;

import org.apache.ibatis.session.SqlSession;

public class SelectAnyShard extends Query {

	public SelectAnyShard(QueryEngine engine) {
		super(engine);
	}

	@Override
	public <U> U query(QueryClosure<U> closure) {
		U result = null;
		SqlSession session = engine.openSession(closure.getExecutorType());
		try {
			result = closure.call(session);
		} finally {
			session.close();
		}
		return result;
	}
}
