package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.*;

import java.util.*;

public class Sharding {

    private static final long INVALID_ID = Long.MIN_VALUE;

    private ShardResolver shardResolver;

    private QueryRegistry queryRegistry;

    public Sharding(Configuration config) {
        shardResolver = new ShardResolver(config.getShards(), config.getKeyMapper());
        queryRegistry = config.getQueryRegistry();
    }

    public <V> V execute(int queryType, long id, QueryClosure<V> closure) {
        Query query = queryRegistry.get(queryType);
        List<Connection> connections = resolveShards(id);
        return query.query(closure, connections);
    }

    public <V> V execute(int queryType, QueryClosure<V> closure) {
        return execute(queryType, INVALID_ID, closure);
    }

    private List<Connection> resolveShards(long id) {
        if (id == INVALID_ID) {
            return shardResolver.resolveAll();
        } else {
            Connection connection = shardResolver.resolveId(id);
            List<Connection> list = new ArrayList<Connection>(1);
            list.add(connection);
            return list;
        }
    }
}
