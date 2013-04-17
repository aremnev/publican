package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.Shard;

import java.sql.DriverManager;

public class JdbcShard implements Shard {

    private final long id;
    private final String driver;
    private final String url;
    private final String user;
    private final String password;

    public JdbcShard(long id, String driver, String url, String user, String password) {
        this.id = id;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
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
