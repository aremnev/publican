package net.thumbtack.sharding.test.jdbc;

import fj.F;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.test.common.Entity;
import net.thumbtack.sharding.test.common.EntityDao;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.ShardingFacade;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import static net.thumbtack.sharding.SqlUtil.*;

public class CommonDao implements EntityDao {

    private ShardingFacade sharding;

    public CommonDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    @Override
    public Entity select(final long id) {
        return sharding.selectAny(new QueryClosure<Entity>() {
            @Override
            public Entity call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT * FROM `common` WHERE `id` = "+ id +";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? parseEntity.f(resultSet) : null;
            }
        });
    }

    @Override
    public List<Entity> select(final List<Long> ids) {
        return sharding.selectAny(new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                StringBuilder idsSrt = new StringBuilder();
                for (long id : ids) {
                    idsSrt.append(id).append(",");
                }
                idsSrt.deleteCharAt(idsSrt.length() - 1);
                String sql = "SELECT * FROM `common` WHERE `id` IN (" + idsSrt + ");";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet, parseEntity);
            }
        });
    }

    @Override
    public List<Entity> selectAll() {
        return sharding.selectAny(new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT * FROM `common`;";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet, parseEntity);
            }
        });
    }

    @Override
    public Entity insert(final Entity entity) {
        sharding.updateAll(new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = insertStr(entity);
                int ins = sqlConn.createStatement().executeUpdate(sql);
                return ins > 0;
            }
        });
        return entity;
    }

    @Override
    public List<Entity> insert(final List<Entity> entities) {
        sharding.updateAll(new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (Entity entity : entities) {
                    statement.addBatch(insertStr(entity));
                }
                int[] res = statement.executeBatch();
                int ins = 0;
                for (int r : res)
                    ins += r;
                return ins;
            }
        });
        return entities;
    }

    @Override
    public boolean update(final Entity entity) {
        return sharding.updateAll(new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = updateStr(entity);
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    @Override
    public boolean update(final List<Entity> entities) {
        sharding.updateAll(new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (Entity entity : entities) {
                    statement.addBatch(updateStr(entity));
                }
                int[] res = statement.executeBatch();
                int ins = 0;
                for (int r : res)
                    ins += r;
                return ins;
            }
        });
        return true;
    }

    @Override
    public boolean delete(Entity entity) {
        return delete(entity.id);
    }

    @Override
    public boolean delete(final long id) {
        return sharding.updateAll(new QueryClosure<Boolean>() {
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
    public boolean delete(final List<Long> ids) {
        sharding.updateAll(new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (long id : ids) {
                    statement.addBatch(deleteStr(id));
                }
                int[] res = statement.executeBatch();
                int ins = 0;
                for (int r : res)
                    ins += r;
                return ins;
            }
        });
        return true;
    }

    @Override
    public boolean deleteAll() {
        return sharding.updateAll(new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "DELETE FROM `common`";
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    private F<ResultSet, Entity> parseEntity = new F<ResultSet, Entity>() {
        @Override
        public Entity f(ResultSet resultSet) {
            try {
                return new Entity(
                        resultSet.getLong(1),
                        resultSet.getString(2),
                        Timestamp.valueOf(resultSet.getString(3))
                );
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private String insertStr(Entity entity) {
        Timestamp time  = new Timestamp(entity.date.getTime());
        return "INSERT INTO `common` (`id`,`text`, `date`) VALUES (" + entity.id + ",'" + entity.text + "', '" + time + "');";
    }

    private String updateStr(Entity entity) {
        Timestamp time = new Timestamp(entity.date.getTime());
        return "UPDATE `common` SET `text` = '" + entity.text + "', `date` = '" + time + "' WHERE `id` = " + entity.id;
    }

    private String deleteStr(long id) {
        return "DELETE FROM `common` WHERE `id` = " + id;
    }
}
