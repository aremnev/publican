package net.thumbtack.sharding.test.cluster;

import fj.F;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.test.common.Entity;
import net.thumbtack.sharding.test.common.EntityDao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.thumbtack.helper.Util.index;
import static net.thumbtack.sharding.TestUtil.parseEntities;

public class ShardedDao implements EntityDao {

    private ShardingFacade sharding;

    public ShardedDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    @Override
    public Entity select(final long id) {
        return sharding.selectSpec(id, getSelectClosure(id));
    }

    @Override
    public List<Entity> select(List<Long> ids) {
        return sharding.selectAll(ids, new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                List<Long> ids = connection.getCargo();
                StringBuilder idsSrt = new StringBuilder();
                for (long id : ids) {
                    idsSrt.append(id).append(",");
                }
                idsSrt.deleteCharAt(idsSrt.length() - 1);
                String sql = "SELECT * FROM `sharded` WHERE `id` IN (" + idsSrt + ");";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet, parseEntity);
            }
        });
    }

    @Override
    public List<Entity> selectAll() {
        return sharding.selectAll(new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT * FROM `sharded`;";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet, parseEntity);
            }
        });
    }

    @Override
    public Entity insert(final Entity entity) {
        sharding.updateSpec(entity.id, new QueryClosure<Object>() {
            @Override
            public Object call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = insertStr(entity);
                int ins = sqlConn.createStatement().executeUpdate(sql);
                return ins > 0;
            }
        });
        return entity;
    }

    @Override
    public List<Entity> insert(List<Entity> entities) {
        final Map<Long, Entity> entityMap = index(entities, new F<Entity, Long>() {
            @Override
            public Long f(Entity entity) {
                return entity.getId();
            }
        });
        sharding.updateAll(new ArrayList<Long>(entityMap.keySet()), new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (long id : ids) {
                    statement.addBatch(insertStr(entityMap.get(id)));
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
        return sharding.updateSpec(entity.id, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Timestamp time  = new Timestamp(entity.date.getTime());
                String sql = "UPDATE `sharded` SET `text` = '"+ entity.text +"', `date` = '"+ time +"' WHERE `id` = "+ entity.id;
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    @Override
    public boolean update(List<Entity> entities) {
        final Map<Long, Entity> entityMap = index(entities, new F<Entity, Long>() {
            @Override
            public Long f(Entity entity) {
                return entity.getId();
            }
        });
        sharding.updateAll(new ArrayList<Long>(entityMap.keySet()), new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                Statement statement = sqlConn.createStatement();
                for (long id : ids) {
                    statement.addBatch(updateStr(entityMap.get(id)));
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
        sharding.updateAll(ids, new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
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
                String sql = "DELETE FROM `sharded`";
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    public static F<ResultSet, Entity> parseEntity = new F<ResultSet, Entity>() {
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

    public QueryClosure<Entity> getSelectClosure(final long id) {
        return new QueryClosure<Entity>() {
            @Override
            public Entity call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = "SELECT * FROM `sharded` WHERE `id` = " + id + ";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? parseEntity.f(resultSet) : null;
            }
        };
    }

    public static String insertStr(Entity entity) {
        Timestamp time  = new Timestamp(entity.date.getTime());
        return "INSERT INTO `sharded` (`id`,`text`, `date`, `timestamp`) VALUES (" + entity.id + ",'" + entity.text + "', '" + time + "', '" + System.currentTimeMillis() + "');";
    }

    public static String updateStr(Entity entity) {
        Timestamp time = new Timestamp(entity.date.getTime());
        return "UPDATE `sharded` SET `text` = '" + entity.text + "', `date` = '" + time + "' WHERE `id` = " + entity.id;
    }

    public static String deleteStr(long id) {
        return "DELETE FROM `sharded` WHERE `id` = " + id;
    }
}
