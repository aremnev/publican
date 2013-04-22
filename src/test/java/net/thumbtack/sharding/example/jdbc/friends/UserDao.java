package net.thumbtack.sharding.example.jdbc.friends;

import fj.F;
import net.thumbtack.sharding.Dao;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.test.jdbc.JdbcConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static net.thumbtack.sharding.SqlUtil.*;

public class UserDao implements Dao<User> {

    private ShardingFacade sharding;

    public UserDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    @Override
    public User select(final long id) {
        return sharding.selectSpec(id, new QueryClosure<User>() {
            @Override
            public User call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT * FROM `users` WHERE `id` = " + id + ";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? parseUser.f(resultSet) : null;
            }
        });
    }

    @Override
    public List<User> select(final List<Long> ids) {
        return sharding.selectAll(new QueryClosure<List<User>>() {
            @Override
            public List<User> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                StringBuilder idsSrt = new StringBuilder();
                for (long id : ids) {
                    idsSrt.append(id).append(",");
                }
                idsSrt.deleteCharAt(idsSrt.length() - 1);
                String sql = "SELECT * FROM `users` WHERE `id` IN (" + idsSrt + ");";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet, parseUser);
            }
        });
    }

    @Override
    public List<User> selectAll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public User insert(User entity) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<User> insert(List<User> entities) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean update(User entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(User entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(long id) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean deleteAll() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private F<ResultSet, User> parseUser = new F<ResultSet, User>() {
        @Override
        public User f(ResultSet resultSet) {
            try {
                return new User(
                        resultSet.getLong(1),
                        resultSet.getString(2),
                        resultSet.getString(3),
                        Timestamp.valueOf(resultSet.getString(4))
                );
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };
}
