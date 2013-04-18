package net.thumbtack.sharding;

import net.thumbtack.sharding.common.Storage;
import net.thumbtack.sharding.jdbc.JdbcStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingSuite {

    private static final Logger logger = LoggerFactory.getLogger(ShardingSuite.class);

    H2Server h2Server = new H2Server();

    public Storage jdbcStorage;

    public ShardingSuite() throws Exception {
        jdbcStorage = new JdbcStorage();
    }

    public void start() throws Exception {
        h2Server.start();
        logger.debug("embedded db server started");
    }

    public void stop() throws Exception {
        h2Server.stop();
        logger.debug("embedded db server stopped");
    }
}
