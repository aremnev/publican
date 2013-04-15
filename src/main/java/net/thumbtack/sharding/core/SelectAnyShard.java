package net.thumbtack.sharding.core;

import net.thumbtack.helper.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class SelectAnyShard extends Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectAnyShard.class);

    private Random random;

    public SelectAnyShard(Random random) {
        this.random = random;
    }

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        Connection connection = Util.getRandom(random, shards);
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
