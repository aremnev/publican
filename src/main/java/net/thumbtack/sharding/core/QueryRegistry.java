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

    private Map<Integer, Query> registry = new HashMap<Integer, Query>();

    @SuppressWarnings("unchecked")
    public QueryRegistry (Properties props) {
        int threads = Integer.parseInt(props.getProperty(WORK_THREADS, "0"));
        ExecutorService queryExecutor = null;
        if (threads > 0) {
            queryExecutor = Executors.newFixedThreadPool(threads, new NamedThreadFactory("query"));
        }
        Set<Integer> queryIds = new HashSet<Integer>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(QUERY)) {
                int id = Integer.parseInt(name.split("\\.")[1]);
                queryIds.add(id);
            }
        }
        for (int queryId : queryIds) {
            try {
                boolean isSync = Boolean.parseBoolean(props.getProperty(QUERY + queryId + SYNCHRONOUS, "false"));
                String className = props.getProperty(QUERY + queryId + CLASS);
                Class<Query> clazz = (Class<Query>) Class.forName(className);
                Query query;
                if (isSync) {
                    Constructor<Query> ctor = clazz.getConstructor();
                    query = ctor.newInstance();
                } else {
                    Constructor<Query> ctor = clazz.getConstructor(ExecutorService.class);
                    query = ctor.newInstance(queryExecutor);
                }
                registry.put(queryId, query);
            } catch (Exception e) {
                logger.error("Failed to load query with id " + queryId);
            }
        }
    }

    public void register(int queryType, Query query) {
        registry.put(queryType, query);
    }

    public Query get(int queryType) {
        return registry.get(queryType);
    }
}
