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
     * Retrieves the several objects by primary key.
     *
     * @param ids
     *            Primary keys of the objects to fetch.
     * @return The list objects, or empty list if no such objects were found.
     */
    List<T> select(List<Long> ids);

    /**
     * Retrieve all the entities in the database (use with care).
     *
     * @return A list of every Entity in the database.
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
     * Inserts a batch of new object record into the database.
     *
     * @param entities
     *            Objects to insert. Must be new and have no primary key. Its
     *            primary key will be set during insert.
     * @return The object inserted with its primary key set.
     */
    List<T> insert(List<T> entities);

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
     * Updates a butch of objects in the database.
     *
     * @param entities
     *            Objects to update.
     * @return <code>true</code> if instance updated, <code>false</code>
     *         otherwise.
     */
    boolean update(List<T> entities);

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
     * Deletes a batch of objects from the database.
     *
     * @param ids
     *            Primary keys of the objects to delete.
     * @return <code>true</code> if instance updated, <code>false</code>
     *         otherwise.
     */
    boolean delete(List<Long> ids);

    /**
     * Deletes all objects from the database. Use with extreme care!
     */
    boolean deleteAll();
}