package net.thumbtack.sharding;

import net.thumbtack.sharding.common.StorageServer;
import net.thumbtack.sharding.core.*;
import net.thumbtack.sharding.jdbc.JdbcShard;
import net.thumbtack.sharding.jdbc.JdbcStorageServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ShardingSuite {

    private static final Logger logger = LoggerFactory.getLogger(ShardingSuite.class);

    public StorageServer jdbcServer;

    public void start() throws Exception {
        jdbcServer = new JdbcStorageServer();
        jdbcServer.start();
        logger.debug("embedded db server started");
    }

    public void stop() throws Exception {
        jdbcServer.stop();
        logger.debug("embedded db server stopped");
    }
}
