package net.thumbtack.sharding.query;

import org.apache.ibatis.session.SqlSession;

public class UpdateSpecShard extends Query {

	public UpdateSpecShard(QueryEngine engine) {
		super(engine);
	}

	@Override
	public <U> U query(QueryClosure<U> closure, long id) {
		U result = null;
		SqlSession session = engine.openSession(id, closure.getExecutorType());
		try {
			result = closure.call(session);
			session.commit();
		} catch (RuntimeException e) {
			session.rollback();
			throw e;
		} finally {
			session.close();
		}
		return result;
	}
}
