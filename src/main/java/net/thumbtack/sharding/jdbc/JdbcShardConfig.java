package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.ShardConfig;

public class JdbcShardConfig extends ShardConfig {

    private String driver;
    private String url;
    private String user;
    private String password;

    public JdbcShardConfig() {
        super();
    }

    public JdbcShardConfig(long id, String driver, String url, String user, String password) {
        super(id);
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
