package net.thumbtack.sharding.query;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

public abstract class QueryClosure<V>{
	private ExecutorType executorType;

	public QueryClosure() {
		this(ExecutorType.SIMPLE);
	}

	public QueryClosure(ExecutorType executorType) {
		this.executorType = executorType;
	}

	public ExecutorType getExecutorType() {
		return executorType;
	}

	public abstract V call(Transaction transaction);
}
