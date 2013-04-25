package net.thumbtack.sharding.example.jdbc.friends;

import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FriendsDao {

    private ShardingFacade sharding;

    public FriendsDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    public List<Long> select(final long userId) {
        return sharding.selectSpec(userId, new QueryClosure<List<Long>>() {
            @Override
            public List<Long> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT `friendId` FROM `friends` WHERE `userId` = " + userId +";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                List<Long> result = new ArrayList<Long>();
                while (resultSet.next()) {
                    result.add(resultSet.getLong(1));
                }
                return result;
            }
        });
    }

    public Long select(final long userId, final long friendId) {
        return sharding.selectSpec(userId, new QueryClosure<Long>() {
            @Override
            public Long call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT `friendId` FROM `friends` WHERE `userId` = " + userId +" AND `friendId` = "+ friendId +";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? resultSet.getLong(1) : null;
            }
        });
    }

    public void delete(final long userId, final long friendId) {
        sharding.updateSpec(userId, new QueryClosure<Object>() {
            @Override
            public Object call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "DELETE FROM `friends` WHERE `userId` = "+ userId +" AND `friendId` = "+ friendId +";";
                sqlConn.prepareStatement(sql).executeUpdate();
                return null;
            }
        });
    }

    public void insert(final long userId, final long friendId) {
        sharding.updateSpec(userId, new QueryClosure<Object>() {
            @Override
            public Object call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = insertStr(userId, friendId);
                sqlConn.createStatement().executeUpdate(sql);
                return null;
            }
        });
    }

    public void insert(List<Long> userIds, List<Long> friendIds) {
        final Map<Long, List<Long>> map = new HashMap<Long, List<Long>>();
        for (int i = 0; i < userIds.size(); i++) {
            long userId = userIds.get(i);
            List<Long> fIds = map.get(userId);
            if (fIds == null) {
                fIds = new ArrayList<Long>();
                map.put(userId, fIds);
            }
            fIds.add(friendIds.get(i));
        }
        sharding.updateAll(new ArrayList<Long>(map.keySet()), new QueryClosure<Object>() {
            @Override
            public Object call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                List<Long> userIds = connection.getCargo();
                Statement statement = sqlConn.createStatement();
                for (long userId : userIds) {
                    List<Long> friendIds = map.get(userId);
                    for (long friendId : friendIds) {
                        statement.addBatch(insertStr(userId, friendId));
                    }
                }
                statement.executeBatch();
                return null;
            }
        });
    }

    private String insertStr(long userId, long friendId) {
        return "INSERT INTO `friends` (`userId`,`friendId`) VALUES ("+ userId +","+ friendId +");";
    }
}
