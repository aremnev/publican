package net.thumbtack.sharding.core.query;

import net.thumbtack.helper.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * Select from any shard.
 */
public class SelectAnyShard implements Query {

    private static final Logger logger = LoggerFactory.getLogger(SelectAnyShard.class);

    private Random random;

    /**
     * Constructor.
     * @param random The random object to select random shard.
     */
    public SelectAnyShard(Random random) {
        this.random = random;
    }

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        Connection connection = Util.getRandom(random, shards);
        if (logger.isDebugEnabled())
            logger.debug(connection.toString());
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
