package net.thumbtack.sharding.core.query;

/**
 * Encapsulates real connection to data storage.
 * Supports operations which used during query life cycle.
 */
public abstract class Connection {

    private Object cargo;

    /**
     * Opens the connection.
     */
    public abstract void open();

    /**
     * Commits the changes.
     */
    public abstract void commit();

    /**
     * Rollbacks the changes.
     */
    public abstract void rollback();

    /**
     * Closes the connection.
     */
    public abstract void close();

    /**
     * Sets the additional information to sent it to the closure.
     * @param cargo The cargo.
     */
    public void setCargo(Object cargo) {
        this.cargo = cargo;
    }

    /**
     * Gets the additional information.
     * @param <V> The cargo type.
     * @return The cargo;
     */
    @SuppressWarnings("unchecked")
    public <V> V getCargo() {
        return (V) cargo;
    }
}
