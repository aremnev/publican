package net.thumbtack.sharding.query;

import org.slf4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Map;

public abstract class Query {

	protected QueryEngine engine;

	public Query(QueryEngine engine) {
		this.engine = engine;
	}

	public <U> U query(QueryClosure<U> closure) {
		throw new NotImplementedException();
	}

	public <U> U query(QueryClosure<U> closure, long id) {
		throw new NotImplementedException();
	}

	@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
	protected static void logErrors(Logger logger, Object result, List<QueryError> errors) {
		StringBuilder sb = new StringBuilder();
		for (QueryError error : errors) {
			sb.append("On shard #").append(error.shard).append(":\n");
			if (error.throwable != null) {
				if (error.throwable.getMessage() != null) {
					sb.append(error.throwable.getMessage()).append("\n");
				}
				sb.append(printStackTrace(error.throwable.getStackTrace()));
			}
			sb.append("Parent stack:\n").append(printStackTrace(error.parentStackTrace));
		}
		logger.error("Query was finished with result {} end errors\n{}", result, sb.toString());
	}

	public static String printStackTrace(StackTraceElement[] stackTrace) {
		if (stackTrace != null) {
			StringBuilder sb = new StringBuilder();
			for (StackTraceElement trace : stackTrace)
				sb.append("\tat ").append(trace).append("\n");

			return sb.toString();
		}
		return "";
	}

}
