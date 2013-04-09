package net.thumbtack.sharding;

import org.apache.ibatis.session.SqlSession;

public class SelectSpecShard extends Query {

	public SelectSpecShard(QueryEngine engine) {
		super(engine);
	}

	@Override
	public <U> U query(QueryClosure<U> closure, long id) {
		U result = null;
		SqlSession session = engine.openSession(id, closure.getExecutorType());
		try {
			result = closure.call(session);
		} finally {
			session.close();
		}
		return result;
	}
}
