package net.thumbtack.sharding.jdbc;

import net.thumbtack.sharding.Entity;
import net.thumbtack.sharding.EntityDao;
import net.thumbtack.sharding.core.Connection;
import net.thumbtack.sharding.core.QueryClosure;
import net.thumbtack.sharding.core.Sharding;
import net.thumbtack.sharding.core.ShardingFacade;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static net.thumbtack.sharding.core.Sharding.*;

public class CommonDao implements EntityDao {

    private ShardingFacade sharding;

    public CommonDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    @Override
    public Entity select(long id) {
        return sharding.selectAny(selectByIdClosure(id));
    }

    @Override
    public List<Entity> select(List<Long> ids) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Entity> selectAll() {
        return sharding.selectAny(selectAllClosure());
    }

    @Override
    public Entity insert(Entity Entity) {
        sharding.updateAll(insertClosure(Entity));
        return Entity;
    }

    @Override
    public List<Entity> insert(List<Entity> entities) {
        sharding.updateAll(insertClosure(entities));
        return entities;
    }

    @Override
    public boolean update(Entity Entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(Entity Entity) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delete(long id) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean deleteAll() {
        return false;
    }

    private QueryClosure<Entity> selectByIdClosure(final long id) {
        return new QueryClosure<Entity>() {
            @Override
            public Entity call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "SELECT * FROM `common` WHERE `id` = "+ id +";";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                resultSet.next();
                return new Entity(
                        resultSet.getLong(1),
                        resultSet.getString(2),
                        Timestamp.valueOf(resultSet.getString(3))
                );
            }
        };
    }

    private QueryClosure<List<Entity>> selectAllClosure() {
        return new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "SELECT * FROM `common`;";
                ResultSet resultSet = sqlConn.prepareStatement(sql).executeQuery();
                List<Entity> result = new ArrayList<Entity>();
                while (resultSet.next()) {
                    result.add(new Entity(
                            resultSet.getLong(1),
                            resultSet.getString(2),
                            Timestamp.valueOf(resultSet.getString(3))
                    ));
                }
                return result;
            }
        };
    }

    private QueryClosure<Boolean> insertClosure(final List<Entity> entities) {
        return new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = "";
                for (Entity entity : entities) {
                    sql += insertStr(entity);
                }
                int ins = sqlConn.createStatement().executeUpdate(sql);
                return ins > 0;
            }
        };
    }

    private QueryClosure<Boolean> insertClosure(final Entity entity) {
        return new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                java.sql.Connection sqlConn = (java.sql.Connection) connection.getConnection();
                String sql = insertStr(entity);
                int ins = sqlConn.createStatement().executeUpdate(sql);
                return ins > 0;
            }
        };
    }

    private String insertStr(Entity entity) {
        Timestamp time  = new Timestamp(entity.date.getTime());
        return "INSERT INTO `common` (`id`,`text`, `date`) VALUES (" + entity.id + ",'" + entity.text + "', '" + time.toString() + "');";
    }
}
