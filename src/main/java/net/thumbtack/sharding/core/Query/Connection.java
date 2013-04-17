package net.thumbtack.sharding.core.query;

/**
 * Encapsulates real connection to data storage.
 * Supports operations which used during query life cycle.
 */
public interface Connection {

    /**
     * Opens the connection.
     */
    void open();

    /**
     * Commits the changes.
     */
    void commit();

    /**
     * Rollbacks the changes.
     */
    void rollback();

    /**
     * Closes the connection.
     */
    void close();
}
