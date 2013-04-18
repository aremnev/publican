package net.thumbtack.sharding.core;

import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QueryRegistry {

    private static final Logger logger = LoggerFactory.getLogger(QueryRegistry.class);

    private static final String WORK_THREADS = "workThreads";
    private static final String QUERY = "query.";
    private static final String CLASS = ".class";
    private static final String SYNCHRONOUS = ".synchronous";

    private Map<Long, Query> registry = new HashMap<Long, Query>();

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

    public void register(long queryType, Query query) {
        registry.put(queryType, query);
    }

    public Query get(long queryType) {
        return registry.get(queryType);
    }
}
