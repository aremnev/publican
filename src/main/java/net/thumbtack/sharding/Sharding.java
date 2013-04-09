package net.thumbtack.sharding;

import net.thumbtack.sharding.query.Query;
import net.thumbtack.sharding.query.QueryClosure;

public class Sharding {

	private final long INVALID_ID = Long.MIN_VALUE;

	protected <V> V execute(int queryType, long id, QueryClosure<V> closure) {

	}

	protected <V> V execute(int queryType, QueryClosure<V> closure) {
		return execute(queryType, INVALID_ID, closure);
	}
}
