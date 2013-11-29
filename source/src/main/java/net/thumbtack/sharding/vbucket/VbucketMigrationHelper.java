package net.thumbtack.sharding.vbucket;

import net.thumbtack.sharding.core.query.QueryClosure;

import java.util.List;

public interface VbucketMigrationHelper<T> {

    /**
     * @param idFrom Min id.
     * @param idTo Max id.
     * @return Function that computes last modification time
     *         of entities with id in [idFrom, idTo]
     */
    QueryClosure<Long> getLastModificationTime(long idFrom, long idTo);

    /**
     * @param idFrom Min id.
     * @param idTo Max id.
     * @param timestamp
     * @return Function that returns entities modified
     *         after timestamp and id in [idFrom, idTo]
     */
    QueryClosure<List<Long>> getModifiedAfter(long idFrom, long idTo, long timestamp);

    /**
     * @param id Entity id.
     * @return Function that returns entity with given id.
     */
    QueryClosure<T> getEntity(long id);

    /**
     * @param entity Entity.
     * @return Function that writes entity.
     */
    QueryClosure<Void> putEntity(T entity);

    /**
     * @param idFrom Min id.
     * @param idTo Max id.
     * @return Function that removes entities with id in [idFrom, idTo]
     */
    QueryClosure<Void> remove(long idFrom, long idTo);
}
