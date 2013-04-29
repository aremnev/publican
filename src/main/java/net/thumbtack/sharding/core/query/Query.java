package net.thumbtack.sharding.core.query;

import java.util.List;

/**
 * Any query must implements this interface.
 */
public interface Query {

    /**
     * Executes query.
     * @param closure The object that encapsulates the real action.
     * @param shards The connections with which the query will be executed.
     * @param <U> The type of query result.
     * @return The result of query execution.
     */
    <U> U query(QueryClosure<U> closure, List<Connection> shards);

    /**
     * Does the query write or only read.
     * @return True if the query does update false otherwise.
     */
    boolean isUpdate();
}
