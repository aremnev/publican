package net.thumbtack.sharding.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

class SelectShard extends Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectShard.class);

    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        for (Connection connection : shards) {
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
