package net.thumbtack.sharding.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public abstract class JdbcTest {

    private static final Logger logger = LoggerFactory.getLogger(JdbcTest.class);

    private static H2Server h2Server;

    @BeforeClass
    public static void startEmbeddedDbServer() throws Exception {
        h2Server = new H2Server();
        h2Server.start();
        logger.debug("embedded db server started");
    }

    @AfterClass
    public static void stopEmbeddedDbServer() throws Exception {
        h2Server.stop();
        logger.debug("embedded db server stopped");
    }

    protected Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.h2.Driver");
        return DriverManager.getConnection("jdbc:h2:mem:test0;AUTOCOMMIT=ON;DB_CLOSE_DELAY=-1;MODE=MYSQL;MVCC=TRUE;LOCK_TIMEOUT=120000", "sa", "");
    }
}
