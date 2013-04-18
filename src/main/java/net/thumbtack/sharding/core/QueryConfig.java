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
            QueryConfig config = new QueryConfig();
            config.setId(queryId);
            config.setClazz((Class<Query>) Class.forName(props.getProperty(QUERY + queryId + CLASS)));
            config.setSynchronous(Boolean.parseBoolean(props.getProperty(QUERY + queryId + SYNCHRONOUS, "false")));
            result.add(config);
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
