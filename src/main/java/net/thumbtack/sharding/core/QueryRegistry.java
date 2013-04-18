package net.thumbtack.sharding.core;

import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The query registry. It is used for mapping queryId to query.
 */
public class QueryRegistry {

    private static final Logger logger = LoggerFactory.getLogger(QueryRegistry.class);

    private Map<Long, Query> registry = new HashMap<Long, Query>();

    /**
     * Constructor.
     * @param config The general config.
     */
    @SuppressWarnings("unchecked")
    public QueryRegistry (ShardingConfig config) {
        ExecutorService queryExecutor = null;
        if (config.getWorkThreads() > 0) {
            queryExecutor = Executors.newFixedThreadPool(config.getWorkThreads(), new NamedThreadFactory("query"));
        }
        for (QueryConfig queryConfig : config.getQueryConfigs()) {
            try {
                Query query;
                if (queryConfig.isSynchronous()) {
                    Constructor<Query> ctor = queryConfig.getClazz().getConstructor();
                    query = ctor.newInstance();
                } else {
                    Constructor<Query> ctor = queryConfig.getClazz().getConstructor(ExecutorService.class);
                    query = ctor.newInstance(queryExecutor);
                }
                registry.put(queryConfig.getId(), query);
            } catch (Exception e) {
                logger.error("Failed to load query with id " + queryConfig.getId());
            }
        }
    }

    /**
     * Registers the new query.
     * @param queryId The query id.
     * @param query The query.
     */
    public void register(long queryId, Query query) {
        registry.put(queryId, query);
    }

    /**
     * Get the query by id.
     * @param queryId The query id.
     * @return The query.
     */
    public Query get(long queryId) {
        return registry.get(queryId);
    }
}
