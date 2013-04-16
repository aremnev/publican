package net.thumbtack.sharding.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class SelectAllShardsSum extends Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectAllShardsSum.class);

    @Override
    @SuppressWarnings("unchecked")
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        Integer result = 0;
        for (final Connection connection : shards) {
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
}
