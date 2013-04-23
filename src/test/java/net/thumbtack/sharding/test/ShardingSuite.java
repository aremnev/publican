package net.thumbtack.sharding.test;

import net.thumbtack.sharding.H2Server;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.example.jdbc.friends.FriendsStorage;
import net.thumbtack.sharding.test.jdbc.JdbcStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingSuite {

    private static final Logger logger = LoggerFactory.getLogger(ShardingSuite.class);

    H2Server h2Server = new H2Server();

    public Storage jdbcStorageAsync;
    public Storage jdbcStorageSync;
    public Storage friendsStorage;

    public ShardingSuite() throws Exception {
        jdbcStorageAsync = new JdbcStorage("query-async.properties");
        jdbcStorageSync = new JdbcStorage("query-sync.properties");
        friendsStorage = new FriendsStorage();
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
