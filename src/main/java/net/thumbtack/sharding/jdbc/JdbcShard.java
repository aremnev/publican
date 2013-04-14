package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.core.Connection;
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
    public Connection getConnection() throws Exception {
        return new Connection() {

            private java.sql.Connection sqlConn;

            @Override
            public void open() throws Exception {
                if (sqlConn != null) {
                    throw new Exception("Connection already is opened");
                }
                Class.forName(driver);
                sqlConn = DriverManager.getConnection(url, user, password);
            }

            @Override
            public void commit() throws Exception {
                if (sqlConn != null)
                    sqlConn.commit();
            }

            @Override
            public void rollback() throws Exception {
                if (sqlConn != null)
                    sqlConn.rollback();
            }

            @Override
            public void close() throws Exception {
                if (sqlConn != null)
                    sqlConn.close();
            }
        };
    }
}
