package net.thumbtack.sharding.core.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Update on all shards synchronously.
 */
public class UpdateAllShards implements Query {

    private static final Logger logger = LoggerFactory.getLogger(UpdateAllShards.class);

    @Override
    public <U> U query(QueryClosure<U> closure, List<Connection> shards) {
        U result = null;
        int errors = 0;
        try {
            for (Connection connection : shards) {
                if (logger.isDebugEnabled()) {
                    logger.debug(connection.toString());
                }
                try {
                    connection.open();
                    try {
                        result = closure.call(connection);
                        connection.commit();
                    } catch (RuntimeException e) {
                        try {
                            connection.rollback();
                        } catch (Exception ignored) {}
                        throw e;
                    } finally {
                        connection.close();
                    }
                } catch (Exception e) {
                    logger.error("Failed to execute query on " + connection, e);
                    errors++;
                }
            }
        } finally {
            // check that all shards was updated
            if (errors > 0) {
                logger.error("SHARDS OUT OF SYNC. See above logged errors.");
            }
        }
        return result;
    }
}
