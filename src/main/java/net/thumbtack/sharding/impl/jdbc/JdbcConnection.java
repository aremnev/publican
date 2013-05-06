package net.thumbtack.sharding.impl.jdbc;

import net.thumbtack.sharding.core.query.Connection;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Encapsulates the sql connection.
 */
public class JdbcConnection extends Connection {

    private java.sql.Connection sqlConn;
    private DataSource dataSource;

    /**
     * Constructor.
     * @param dataSource The dataSource.
     */
    public JdbcConnection(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void open() {
        if (sqlConn != null) {
            throw new RuntimeException("Connection already is opened.");
        }
        try {
            sqlConn = dataSource.getConnection();
        } catch (SQLException e) {
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
                "dataSource='" + dataSource + '\'' +
                '}';
    }
}
