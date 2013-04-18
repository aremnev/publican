package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Select from undefined shard. All shards are examined synchronously.
 */
public class SelectShard implements Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectShard.class);

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        for (Connection connection : shards) {
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
            if (result != null) {
                if (result instanceof Collection<?>) {
                    if (((Collection<?>) result).size() > 0) {
                        return result;
                    }
                } else {
                    return result;
                }
            }
        }

        return result;
    }
}
