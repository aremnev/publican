package net.thumbtack.sharding.impl.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.Shard;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.util.*;

/**
 * The jdbc shard.
 */
public class JdbcShard implements Shard {

    /**
     * the min pool size.
     */
    private static final int MIN_POOL_SIZE = 5;

    /**
     * the acquire increment.
     */
    private static final int ACQUIRE_INCREMENT = 5;

    /**
     * the max pool size.
     */
    private static final int MAX_POOL_SIZE = 20;

    private static final String SHARD = "shard.";
    private static final String DRIVER = ".driver";
    private static final String URL = ".url";
    private static final String USER = ".user";
    private static final String PASSWORD = ".password";

    private final int id;


    private DataSource dataSource;

    /**
     * Builds list of jdbc shard configurations from properties.
     * @param props The properties.
     * @return The list of jdbc shard configurations.
     */
    public static List<JdbcShard> fromProperties(Properties props) {
        Set<Integer> shardIds = new HashSet<Integer>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(SHARD)) {
                int id = Integer.parseInt(name.split("\\.")[1]);
                shardIds.add(id);
            }
        }
        List<JdbcShard> result = new ArrayList<JdbcShard>(shardIds.size());
        for (int shardId : shardIds) {
            String driver = props.getProperty(SHARD + shardId + DRIVER);
            String url = props.getProperty(SHARD + shardId + URL);
            String user = props.getProperty(SHARD + shardId + USER);
            String password = props.getProperty(SHARD + shardId + PASSWORD);
            result.add(new JdbcShard(shardId, driver, url, user, password));
        }
        return result;
    }

    /**
     * Constructor.
     * @param id The shard id.
     * @param driver The sql driver full class name.
     * @param url The url to database.
     * @param user The database user.
     * @param password The database password.
     */
    public JdbcShard(int id, String driver, String url, String user, String password) {
        this.id = id;
        dataSource =  setupDataSource(driver, url, user, password);
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Connection getConnection() {
        return new JdbcConnection(dataSource);
    }

    private static DataSource setupDataSource(String driver, String url, String user, String password) {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass(driver);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
        cpds.setJdbcUrl(url);
        cpds.setUser(user);
        cpds.setPassword(password);
        cpds.setMinPoolSize(MIN_POOL_SIZE);
        cpds.setAcquireIncrement(ACQUIRE_INCREMENT);
        cpds.setMaxPoolSize(MAX_POOL_SIZE);
        return cpds;
    }

    @Override
    public String toString() {
        return "JdbcShard{" +
                "id=" + id +
                ", dataSource='" + dataSource + '\'' +
                '}';
    }
}
