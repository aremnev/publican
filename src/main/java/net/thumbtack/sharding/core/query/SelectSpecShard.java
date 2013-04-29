package net.thumbtack.sharding.core.query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Select from specific shard.
 */
public class SelectSpecShard implements Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectSpecShard.class);

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        Connection connection = shards.get(0);
        if (logger.isDebugEnabled()) {
            logger.debug(connection.toString());
        }
        try {
            connection.open();
            try {
                result = closure.call(connection);
            } finally {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("Failed to execute query on " + connection, e);
        }
        return result;
    }

    @Override
    public boolean isUpdate() {
        return false;
    }
}
