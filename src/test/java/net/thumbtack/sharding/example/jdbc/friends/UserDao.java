package net.thumbtack.sharding.example.jdbc.friends;

import fj.F;
import net.thumbtack.sharding.Dao;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.test.jdbc.JdbcConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.thumbtack.sharding.SqlUtil.*;
import static net.thumbtack.helper.Util.*;

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
                String sql = "SELECT * FROM `users` WHERE `id` = "+ id +";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? parseUser.f(resultSet) : null;
            }
        });
    }

    @Override
    public List<User> select(List<Long> ids) {
        return sharding.selectAll(ids, new QueryClosure<List<User>>() {
            @Override
            public List<User> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                List<Long> ids = connection.getCargo();
                StringBuilder idsSrt = new StringBuilder();
                for (long id : ids) {
                    idsSrt.append(id).append(",");
                }
                idsSrt.deleteCharAt(idsSrt.length() - 1);
                String sql = "SELECT * FROM `users` WHERE `id` IN ("+ idsSrt +");";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet, parseUser);
            }
        });
    }

    @Override
    public List<User> selectAll() {
        return sharding.selectAll(new QueryClosure<List<User>>() {
            @Override
            public List<User> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT * FROM `users`;";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet, parseUser);
            }
        });
    }

    @Override
    public User insert(final User user) {
        return sharding.updateSpec(user.getId(), new QueryClosure<User>() {
            @Override
            public User call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = insertStr(user);
                sqlConn.createStatement().executeUpdate(sql);
                return user;
            }
        });
    }

    @Override
    public List<User> insert(final List<User> users) {
        final Map<Long, User> userMap = index(users, new F<User, Long>() {
            @Override
            public Long f(User user) {
                return user.getId();
            }
        });
        return sharding.updateAll(new ArrayList<Long>(userMap.keySet()), new QueryClosure<List<User>>() {
            @Override
            public List<User> call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (long id : ids) {
                    statement.addBatch(insertStr(userMap.get(id)));
                }
                statement.executeBatch();
                return users;
            }
        });
    }

    @Override
    public boolean update(final User user) {
        return sharding.updateSpec(user.getId(), new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = updateStr(user);
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    @Override
    public boolean update(List<User> users) {
        final Map<Long, User> userMap = index(users, new F<User, Long>() {
            @Override
            public Long f(User user) {
                return user.getId();
            }
        });
        return sharding.updateAll(new ArrayList<Long>(userMap.keySet()), new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (long id : ids) {
                    statement.addBatch(updateStr(userMap.get(id)));
                }
                statement.executeBatch();
                return true;
            }
        });
    }

    @Override
    public boolean delete(User user) {
        return delete(user.getId());
    }

    @Override
    public boolean delete(final long id) {
        return sharding.updateSpec(id, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = deleteStr(id);
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    @Override
    public boolean delete(List<Long> ids) {
        return sharding.updateAll(ids, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (long id : ids) {
                    statement.addBatch(deleteStr(id));
                }
                statement.executeBatch();
                return true;
            }
        });
    }

    @Override
    public boolean deleteAll() {
        return sharding.updateAll(new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "DELETE FROM `users`";
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
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

    private String insertStr(User user) {
        Timestamp birthDate  = new Timestamp(user.getBirthDate().getTime());
        return "INSERT INTO `users` (`id`, `email`, `name`, `birthDate`) VALUES ("+
                user.getId() +",'"+ user.getEmail() +"', '"+ user.getName() +"', '"+ birthDate +"');";
    }

    private String updateStr(User user) {
        Timestamp birthDate  = new Timestamp(user.getBirthDate().getTime());
        return "UPDATE `users` " +
                "SET `email` = '"+ user.getEmail() +"', `name` = '"+ user.getName() +"', `birthDate` = '"+ birthDate +
                "' WHERE `id` = "+ user.getId();
    }

    private String deleteStr(long id) {
        return "DELETE FROM `users` WHERE `id` = "+ id;
    }
}
