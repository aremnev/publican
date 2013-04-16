package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.Entity;
import net.thumbtack.sharding.EntityDao;
import net.thumbtack.sharding.core.Connection;
import net.thumbtack.sharding.core.QueryClosure;
import net.thumbtack.sharding.core.ShardingFacade;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

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
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "SELECT * FROM `common` WHERE `id` = "+ id +";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return resultSet.next() ? parseEntity(resultSet) : null;
            }
        });
    }

    @Override
    public List<Entity> select(final List<Long> ids) {
        return sharding.selectAny(new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                StringBuilder idsSrt = new StringBuilder();
                for (long id : ids) {
                    idsSrt.append(id).append(",");
                }
                idsSrt.deleteCharAt(idsSrt.length() - 1);
                String sql = "SELECT * FROM `common` WHERE `id` IN (" + idsSrt + ");";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet);
            }
        });
    }

    @Override
    public List<Entity> selectAll() {
        return sharding.selectAny(new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "SELECT * FROM `common`;";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                return parseEntities(resultSet);
            }
        });
    }

    @Override
    public Entity insert(final Entity entity) {
        sharding.updateAll(new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
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
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
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
        return sharding.updateSpec(entity.id, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                Timestamp time  = new Timestamp(entity.date.getTime());
                String sql = "UPDATE `common` SET `text` = '"+ entity.text +"', `date` = '"+ time +"' WHERE `id` = "+ entity.id;
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
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
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "DELETE FROM `common` WHERE `id` = "+ id;
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    @Override
    public boolean deleteAll() {
        return sharding.updateAll(new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "DELETE FROM `common`";
                int upd = sqlConn.prepareStatement(sql).executeUpdate();
                return upd > 0;
            }
        });
    }

    private Entity parseEntity(ResultSet resultSet) throws SQLException {
        return new Entity(
                resultSet.getLong(1),
                resultSet.getString(2),
                Timestamp.valueOf(resultSet.getString(3))
        );
    }

    private List<Entity> parseEntities(ResultSet resultSet) throws SQLException {
        List<Entity> result = new ArrayList<Entity>();
        while (resultSet.next()) {
            result.add(parseEntity(resultSet));
        }
        return result;
    }

    private String insertStr(Entity entity) {
        Timestamp time  = new Timestamp(entity.date.getTime());
        return "INSERT INTO `common` (`id`,`text`, `date`) VALUES (" + entity.id + ",'" + entity.text + "', '" + time + "');";
    }
}
