package net.thumbtack.sharding;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.LinkedList;
import java.util.List;

public class SelectAllShards extends Query {

	public SelectAllShards(QueryEngine engine) {
		super(engine);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <U> U query(QueryClosure<U> closure) {
		List<Object> result = new LinkedList<Object>();
		for (int shardKey : engine.shardsKeys()) {
			SqlSession session = engine.openSession(shardKey, closure.getExecutorType());
			try {
				U elements = closure.call(session);
				if (elements != null){
					result.addAll((List) elements);
				}
			} finally {
				session.close();
			}
		}
		return (U) result;
	}
}
