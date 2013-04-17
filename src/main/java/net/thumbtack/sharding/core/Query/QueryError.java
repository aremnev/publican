package net.thumbtack.sharding.core.query;

/**
 * Error of the query on the definite shard.
 */
public class QueryError {

    private String shard;

    private Throwable error;

    private StackTraceElement[] parentStackTrace;

    /**
     * Constructor.
     * @param shard The shard which failed.
     * @param error The exception which occurred.
     * @param parentStackTrace The stack trace from thread which executes query
     *                         (not the part of the query on the definite shard).
     */
    public QueryError(String shard, Throwable error, StackTraceElement[] parentStackTrace) {
        this.shard = shard;
        this.error = error;
        this.parentStackTrace = parentStackTrace;
    }

    /**
     * Get shard.
     * @return The shard which failed.
     */
    public String getShard() {
        return shard;
    }

    /**
     * Get error.
     * @return he exception which occurred.
     */
    public Throwable getError() {
        return error;
    }

    /**
     * Get parent stack trace.
     * @return The stack trace from thread which executes query (not the part of the query on the definite shard).
     */
    public StackTraceElement[] getParentStackTrace() {
        return parentStackTrace;
    }
}
