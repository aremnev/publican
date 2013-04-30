package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Select from all shards synchronously with union of results to list.
 */
public class SelectAllShards implements Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectAllShards.class);

    @Override
    @SuppressWarnings("unchecked")
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        List<Object> result = new LinkedList<Object>();
        for (Connection connection : shards) {
            if (logger.isDebugEnabled()) {
                logger.debug(connection.toString());
            }
            try {
                connection.open();
                try {
                    U elements = closure.call(connection);
                    if (elements != null) {
                        result.addAll((List) elements);
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
