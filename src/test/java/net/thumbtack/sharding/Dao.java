package net.thumbtack.sharding;

import java.util.List;


/**
 * Base interface for data access objects that support CRUD operations.
 *
 * @param <T>
 *            The type of object on which to operate.
 */
public interface Dao<T> {
    /**
     * Retrieves an object by primary key.
     *
     * @param id
     *            Primary key of the object to fetch.
     * @return The object, or {@code null} if no such object was found.
     */
    T select(long id);

    /**
     * Retrieve all the entities in the database (use with care).
     *
     * @return A list of every entity in the database.
     */
    List<T> selectAll();

    /**
     * Inserts a new object record into the database.
     *
     * @param entity
     *            Object to insert. Must be new and have no primary key. Its
     *            primary key will be set during insert.
     * @return The object inserted with its primary key set.
     */
    T insert(T entity);

    /**
     * Updates an object in the database.
     *
     * @param entity
     *            Object to update.
     * @return <code>true</code> if instance updated, <code>false</code>
     *         otherwise.
     */
    boolean update(T entity);

    /**
     * Deletes an object from the database.
     *
     * @param entity
     *            Object to delete.
     * @return <code>true</code> if instance updated, <code>false</code>
     *         otherwise.
     */
    boolean delete(T entity);

    /**
     * Deletes an object from the database.
     *
     * @param id
     *            Primary key of the object to delete.
     * @return <code>true</code> if instance updated, <code>false</code>
     *         otherwise.
     */
    boolean delete(long id);

    /**
     * Deletes all objects from the database. Use with extreme care!
     */
    Void deleteAll();
}