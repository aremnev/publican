package net.thumbtack.sharding.core.query;

import java.util.List;

/**
 * Exception of query's execution.
 */
public class QueryException extends RuntimeException {

    private static final long serialVersionUID = -6003272694020918874L;

    private List<QueryError> errors;

    /**
     * Constructor.
     * @param message The message.
     * @param errors The list of errors.
     */
    public QueryException(String message, List<QueryError> errors) {
        super(message);
        this.errors = errors;
    }

    /**
     * Get errors.
     * @return The list of errors.
     */
    public List<QueryError> getErrors() {
        return errors;
    }
}
