package net.thumbtack.shardcon.chunk.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import net.thumbtack.helper.Util;
import net.thumbtack.shardcon.chunk.ConfigurationBroker;
import net.thumbtack.shardcon.common.H2Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class JdbcConfigurationBrokerTest {

    private static H2Server h2Server;

    private static ConfigurationBroker configurationBroker;

    @BeforeClass
    public static void init() throws Exception {
        h2Server = new H2Server();
        h2Server.start();
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("org.h2.Driver");
        dataSource.setJdbcUrl("jdbc:h2:mem:test;AUTOCOMMIT=ON;DB_CLOSE_DELAY=-1;MODE=MYSQL");
        dataSource.setUser("sa");
        dataSource.setPassword("");

        Properties queriesConfig = new Properties();
        queriesConfig.load(Util.getResourceAsReader("jdbc-queries.properties"));
        configurationBroker = new JdbcConfigurationBroker(dataSource, queriesConfig);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        h2Server.stop();
    }

    @Test
    public void getVersionTest() throws Exception {
        long version = configurationBroker.getVersion();
        assertEquals(1, version);
    }
}
