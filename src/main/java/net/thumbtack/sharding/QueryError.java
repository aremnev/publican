package net.thumbtack.sharding;

public class QueryError {

	public final String shard;

	public final Throwable throwable;

	public final StackTraceElement[] parentStackTrace;

	public QueryError(String shard, Throwable throwable, StackTraceElement[] parentStackTrace) {
		this.shard = shard;
		this.throwable = throwable;
		this.parentStackTrace = parentStackTrace;
	}
}
