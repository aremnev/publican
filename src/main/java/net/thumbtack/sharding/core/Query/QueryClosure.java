package net.thumbtack.sharding.core.query;

/**
 * The object that encapsulates the real action.
 * @param <V> The type of the result.
 */
public interface QueryClosure<V> {

    /**
     * Does the action.
     * @param connection The connection.
     * @return The result.
     * @throws Exception if something went wrong.
     */
    V call(Connection connection) throws Exception;
}
