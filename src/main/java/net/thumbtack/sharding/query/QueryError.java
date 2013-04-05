package net.thumbtack.sharding.query;

public class QueryError {

	public final int shard;

	public final Throwable throwable;

	public final StackTraceElement[] parentStackTrace;

	public QueryError(int shard, Throwable throwable, StackTraceElement[] parentStackTrace) {
		this.shard = shard;
		this.throwable = throwable;
		this.parentStackTrace = parentStackTrace;
	}
}
