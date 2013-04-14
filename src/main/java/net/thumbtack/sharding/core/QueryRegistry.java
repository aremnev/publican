package net.thumbtack.sharding.core;

import java.util.HashMap;
import java.util.Map;

public class QueryRegistry {

    private Map<Integer, Query> registrySync = new HashMap<Integer, Query>();
    private Map<Integer, Query> registryAsync = new HashMap<Integer, Query>();

    // synchronous/asynchronous mode
    private boolean sync = false;

    public void register(int queryType, boolean sync, Query query) {
        if (sync) {
            registrySync.put(queryType, query);
        } else {
            registryAsync.put(queryType, query);
        }
    }

    public void setSynchronousMode(boolean sync) {
        this.sync = sync;
    }

    public Query get(int queryType) {
        return get(queryType, sync);
    }

    public Query get(int queryType, boolean sync) {
        if (sync) {
            return registrySync.get(queryType);
        } else {
            return registryAsync.get(queryType);
        }
    }
}
