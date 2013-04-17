package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;

import java.util.List;

public abstract class Query {

    public abstract  <U> U query(QueryClosure<U> closure, List<Connection> shards);

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected static void logErrors(Logger logger, Object result, List<QueryError> errors) {
        StringBuilder sb = new StringBuilder();
        for (QueryError error : errors) {
            sb.append("On shard #").append(error.getShard()).append(":\n");
            if (error.getError() != null) {
                if (error.getError().getMessage() != null) {
                    sb.append(error.getError().getMessage()).append("\n");
                }
                sb.append(printStackTrace(error.getError().getStackTrace()));
            }
            sb.append("Parent stack:\n").append(printStackTrace(error.getParentStackTrace()));
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
