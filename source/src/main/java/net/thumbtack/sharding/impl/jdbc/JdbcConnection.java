package net.thumbtack.sharding.impl.jdbc;

import net.thumbtack.sharding.core.query.Connection;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Encapsulates the sql connection.
 */
public class JdbcConnection extends Connection {

    private java.sql.Connection sqlConn;
    private String driver;
    private String url;
    private String user;
    private String password;

    /**
     * Constructor.
     * @param driver The sql driver full class name.
     * @param url The url to database.
     * @param user The database user.
     * @param password The database password.
     */
    public JdbcConnection(String driver, String url, String user, String password) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open() {
        if (sqlConn != null) {
            throw new RuntimeException("Connection already is opened.");
        }
        try {
            Class.forName(driver);
            sqlConn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            throw new RuntimeException("Failed to open connection.", e);
        }
    }

    @Override
    public void commit() {
        if (sqlConn != null) {
            try {
                sqlConn.commit();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to commit.", e);
            }
        }
    }

    @Override
    public void rollback() {
        if (sqlConn != null) {
            try {
                sqlConn.rollback();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to rollback.", e);
            }
        }
    }

    @Override
    public void close() {
        if (sqlConn != null) {
            try {
                sqlConn.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to rollback.", e);
            }
        }
    }

    /**
     * Gets the sql connection.
     * @return The sql connection.
     */
    public java.sql.Connection getConnection() {
        return sqlConn;
    }

    @Override
    public String toString() {
        return "JdbcConnection{" +
                "url='" + url + '\'' +
                ", driver='" + driver + '\'' +
                '}';
    }
}
