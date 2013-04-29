package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Select from all shards synchronously with union of results to list.
 */
public class SelectAllShardsSum implements Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectAllShardsSum.class);

    @Override
    @SuppressWarnings("unchecked")
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        Integer result = 0;
        for (final Connection connection : shards) {
            if (logger.isDebugEnabled()) {
                logger.debug(connection.toString());
            }
            try {
                connection.open();
                try {
                    Integer res = (Integer) closure.call(connection);
                    if (res != null) {
                        result += res;
                    }
                } finally {
                    connection.close();
                }
            } catch (Exception e) {
                logger.error("Failed to execute query on " + connection, e);
            }
        }
        return (U) result;
    }

    @Override
    public boolean isUpdate() {
        return false;
    }
}
