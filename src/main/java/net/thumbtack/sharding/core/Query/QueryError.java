package net.thumbtack.sharding.core.query;

public class QueryError {

    private String shard;

    private Throwable error;

    private StackTraceElement[] parentStackTrace;

    public QueryError(String shard, Throwable error, StackTraceElement[] parentStackTrace) {
        this.shard = shard;
        this.error = error;
        this.parentStackTrace = parentStackTrace;
    }

    public String getShard() {
        return shard;
    }

    public Throwable getError() {
        return error;
    }

    public StackTraceElement[] getParentStackTrace() {
        return parentStackTrace;
    }
}
