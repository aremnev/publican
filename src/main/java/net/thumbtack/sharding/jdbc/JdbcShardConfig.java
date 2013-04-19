package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.ShardConfig;

import java.util.*;

/**
 * The jdbc shard configuration.
 */
public class JdbcShardConfig extends ShardConfig {

    private static final String SHARD = "shard.";
    private static final String DRIVER = ".driver";
    private static final String URL = ".url";
    private static final String USER = ".user";
    private static final String PASSWORD = ".password";

    private String driver;
    private String url;
    private String user;
    private String password;

    /**
     * Builds list of jdbc shard configurations from properties.
     * @param props The properties.
     * @return The list of jdbc shard configurations.
     */
    public static List<JdbcShardConfig> fromProperties(Properties props) {
        Set<Long> shardIds = new HashSet<Long>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(SHARD)) {
                long id = Long.parseLong(name.split("\\.")[1]);
                shardIds.add(id);
            }
        }
        List<JdbcShardConfig> result = new ArrayList<JdbcShardConfig>(shardIds.size());
        for (long shardId : shardIds) {
            JdbcShardConfig config = new JdbcShardConfig();
            config.setId(shardId);
            config.setDriver(props.getProperty(SHARD + shardId + DRIVER));
            config.setUrl(props.getProperty(SHARD + shardId + URL));
            config.setUser(props.getProperty(SHARD + shardId + USER));
            config.setPassword(props.getProperty(SHARD + shardId + PASSWORD));
            result.add(config);
        }
        return result;
    }

    /**
     * Default constructor.
     */
    public JdbcShardConfig() {
        super();
    }

    /**
     * Constructor.
     * @param id The shard id.
     * @param driver The sql driver full class name.
     * @param url The url to database.
     * @param user The database user.
     * @param password The database password.
     */
    public JdbcShardConfig(long id, String driver, String url, String user, String password) {
        super(id);
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * Gets the sql driver full class name.
     * @return The sql driver full class name.
     */
    public String getDriver() {
        return driver;
    }

    /**
     * Sets the sql driver full class name.
     * @param driver The sql driver full class name.
     */
    public void setDriver(String driver) {
        this.driver = driver;
    }

    /**
     * Gets the url to database.
     * @return The url to database.
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets the url to database.
     * @param url The url to database.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Gets the database user.
     * @return The database user.
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the database user.
     * @param user The database user.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Gets the database password.
     * @return The database password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the database password.
     * @param password The database password.
     */
    public void setPassword(String password) {
        this.password = password;
    }
}
