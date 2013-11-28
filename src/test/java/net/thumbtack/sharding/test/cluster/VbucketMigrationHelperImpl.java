package net.thumbtack.sharding.test.cluster;

import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.test.common.Entity;
import net.thumbtack.sharding.vbucket.VbucketMigrationHelper;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class VbucketMigrationHelperImpl implements VbucketMigrationHelper<Entity> {

    @Override
    public QueryClosure<Long> getLastModificationTime(final long idFrom, final long idTo) {
        return new QueryClosure<Long>() {
            @Override
            public Long call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT `timestamp` FROM `sharded`" +
                        " WHERE `id` >= " + idFrom +
                        " AND id <= " + idTo +
                        " ORDER BY `timestamp` DESC LIMIT 1;";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? resultSet.getLong(1) : null;
            }
        };
    }

    @Override
    public QueryClosure<List<Long>> getModifiedAfter(final long idFrom, final long idTo, final long timestamp) {
        return new QueryClosure<List<Long>>() {
            @Override
            public List<Long> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT `id` FROM `sharded`" +
                        " WHERE `id` >= " + idFrom +
                        " AND `id` <= " + idTo +
                        " AND `timestamp` > '" + timestamp + "';";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                List<Long> result = new ArrayList<Long>(resultSet.getFetchSize());
                while (resultSet.next()) {
                    result.add(resultSet.getLong(1));
                }
                return result;
            }
        };
    }

    @Override
    public QueryClosure<Entity> getEntity(final long id) {
        return new QueryClosure<Entity>() {
            @Override
            public Entity call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT * FROM `sharded` WHERE `id` = " + id + ";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? ShardedDao.parseEntity.f(resultSet) : null;
            }
        };
    }

    @Override
    public QueryClosure<Void> putEntity(final Entity entity) {
        return new QueryClosure<Void>() {
            @Override
            public Void call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                sqlConn.createStatement().executeUpdate(ShardedDao.deleteStr(entity.id));
                sqlConn.createStatement().executeUpdate(ShardedDao.insertStr(entity));
                return null;
            }
        };
    }

    @Override
    public QueryClosure<Void> remove(final long idFrom, final long idTo) {
        return new QueryClosure<Void>() {
            @Override
            public Void call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "DELETE FROM `sharded` WHERE `id` >= " + idFrom + " AND `id` <= " + idTo + ";";
                sqlConn.prepareStatement(sql).executeUpdate();
                return null;
            }
        };
    }
}
