package net.thumbtack.sharding.vbucket;

import net.thumbtack.sharding.core.query.QueryClosure;

import java.util.List;

public interface VbucketMigrationHelper<T> {

    QueryClosure<Long> getLastModificationTime(long idFrom, long idTo);

    QueryClosure<List<Long>> getModifiedAfter(long timestamp);

    QueryClosure<T> getEntity(long id);

    QueryClosure<Void> setEntity(T entity);
}
