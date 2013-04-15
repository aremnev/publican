package net.thumbtack.sharding.core;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SelectSpecShard extends Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectSpecShard.class);

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        Connection connection = shards.get(0);
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
}
