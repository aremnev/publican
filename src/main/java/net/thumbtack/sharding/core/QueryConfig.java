package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.Query;

import java.util.*;

public class QueryConfig {

    private static final String QUERY = "query.";
    private static final String CLASS = ".class";
    private static final String SYNCHRONOUS = ".synchronous";

    private long id;
    private Class<Query> clazz;
    private boolean isSynchronous;

    @SuppressWarnings("unchecked")
    public static List<QueryConfig> fromProperties(Properties props) throws ClassNotFoundException {
        Set<Long> queryIds = new HashSet<Long>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(QUERY)) {
                long id = Long.parseLong(name.split("\\.")[1]);
                queryIds.add(id);
            }
        }
        List<QueryConfig> result = new ArrayList<QueryConfig>(queryIds.size());
        for (long queryId : queryIds) {
            boolean isSync = Boolean.parseBoolean(props.getProperty(QUERY + queryId + SYNCHRONOUS, "false"));
            String className = props.getProperty(QUERY + queryId + CLASS);
            Class<Query> clazz = (Class<Query>) Class.forName(className);
            result.add(new QueryConfig(queryId, clazz, isSync));
        }
        return result;
    }

    public QueryConfig() {}

    public QueryConfig(long id, Class<Query> clazz, boolean synchronous) {
        this.id = id;
        this.clazz = clazz;
        isSynchronous = synchronous;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Class<Query> getClazz() {
        return clazz;
    }

    public void setClazz(Class<Query> clazz) {
        this.clazz = clazz;
    }

    public boolean isSynchronous() {
        return isSynchronous;
    }

    public void setSynchronous(boolean synchronous) {
        isSynchronous = synchronous;
    }
}
