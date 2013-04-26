package net.thumbtack.sharding.impl.jdbc;

import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.Shard;

import java.util.*;

/**
 * The jdbc shard.
 */
public class JdbcShard implements Shard {

    private static final String SHARD = "shard.";
    private static final String DRIVER = ".driver";
    private static final String URL = ".url";
    private static final String USER = ".user";
    private static final String PASSWORD = ".password";

    private final int id;
    private final String driver;
    private final String url;
    private final String user;
    private final String password;

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
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Connection getConnection() {
        return new JdbcConnection(driver, url, user, password);
    }

    @Override
    public String toString() {
        return "JdbcShard{" +
                "id=" + id +
                ", url='" + url + '\'' +
                ", driver='" + driver + '\'' +
                '}';
    }
}
