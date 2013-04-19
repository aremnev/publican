package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.Query;

import java.util.*;

/**
 * Query configuration. It is used by {@link QueryRegistry} to register query.
 */
public class QueryConfig {

    private static final String QUERY = "query.";
    private static final String CLASS = ".class";
    private static final String SYNCHRONOUS = ".synchronous";

    private long id;
    private Class<Query> clazz;
    private boolean isSynchronous;

    /**
     * Builds list of query configurations from properties.
     * @param props The properties.
     * @return The list of query configurations.
     * @throws ClassNotFoundException if class for some query not found.
     */
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

    /**
     * Default constructor.
     */
    public QueryConfig() {}

    /**
     * Constructor.
     * @param id The query id.
     * @param clazz The query class.
     * @param synchronous Is the query synchronous.
     */
    public QueryConfig(long id, Class<Query> clazz, boolean synchronous) {
        this.id = id;
        this.clazz = clazz;
        isSynchronous = synchronous;
    }

    /**
     * Gets the query id.
     * @return The query id.
     */
    public long getId() {
        return id;
    }

    /**
     * Sets the query id.
     * @param id The query id.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Gets the query class.
     * @return The query class.
     */
    public Class<Query> getClazz() {
        return clazz;
    }

    /**
     * Sets the query class.
     * @param clazz The query class.
     */
    public void setClazz(Class<Query> clazz) {
        this.clazz = clazz;
    }

    /**
     * Is the query synchronous.
     * @return True if synchronous false otherwise.
     */
    public boolean isSynchronous() {
        return isSynchronous;
    }

    /**
     * Sets the query synchronicity.
     * @param synchronous True if synchronous false otherwise.
     */
    public void setSynchronous(boolean synchronous) {
        isSynchronous = synchronous;
    }
}
