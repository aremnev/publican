package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class JdbcTest {

    private static final Logger logger = LoggerFactory.getLogger(JdbcTest.class);

    private static H2Server h2Server;
    private static Sharding sharding;

    @BeforeClass
    public static void startEmbeddedDbServer() throws Exception {
        h2Server = new H2Server();
        h2Server.start();
        logger.debug("embedded db server started");

        int shardsCount = 2;
        final List<Shard> shards = new ArrayList<Shard>(shardsCount);
        for (int i = 0; i < shardsCount; i++) {
            String url = "jdbc:h2:mem:test"+ i + ";AUTOCOMMIT=ON;DB_CLOSE_DELAY=-1;MODE=MYSQL;MVCC=TRUE;LOCK_TIMEOUT=120000";
            shards.add(new JdbcShard(i, "org.h2.Driver", url, "sa", ""));
        }
        final KeyMapper keyMapper = new ModuloKeyMapper(shardsCount);
        Configuration configuration = new Configuration() {
            @Override
            public long getSelectAnyRandomSeed() {
                return 4;
            }

            @Override
            public int getNumberOfWorkerThreads() {
                return 2;
            }

            @Override
            public Iterable<Shard> getShards() {
                return shards;
            }

            @Override
            public KeyMapper getKeyMapper() {
                return keyMapper;
            }
        };
        Sharding sharding = new Sharding(configuration);
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
