package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.Shard;

/**
 * The jdbc shard.
 */
public class JdbcShard implements Shard {

    private final long id;
    private final String driver;
    private final String url;
    private final String user;
    private final String password;

    /**
     * Constructor.
     * @param config The jdbc shard configuration.
     */
    public JdbcShard(JdbcShardConfig config) {
        this.id = config.getId();
        this.driver = config.getDriver();
        this.url = config.getUrl();
        this.user = config.getUser();
        this.password = config.getPassword();
    }

    @Override
    public long getId() {
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
