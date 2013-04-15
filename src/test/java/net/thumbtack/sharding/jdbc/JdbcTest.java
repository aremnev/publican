package net.thumbtack.sharding.jdbc;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.core.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static net.thumbtack.sharding.core.Sharding.*;

public abstract class JdbcTest {

    protected static final Logger logger = LoggerFactory.getLogger(JdbcTest.class);

    private static H2Server h2Server;
    protected static Sharding sharding;

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
        sharding = new Sharding(configuration);
    }

    @AfterClass
    public static void stopEmbeddedDbServer() throws Exception {
        h2Server.stop();
        logger.debug("embedded db server stopped");
    }

    @Before
    public void createScheme() throws Exception {
        sharding.execute(UPDATE_ALL_SHARDS, new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.sharding.core.Connection connection) throws Exception {
                Connection sqlConn = (Connection) connection.getConnection();
                String sql = Util.readResource("initialize.sql");
                sqlConn.createStatement().execute(sql);
                return null;
            }
        });
    }
}
