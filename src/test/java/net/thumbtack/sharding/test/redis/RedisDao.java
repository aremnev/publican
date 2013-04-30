package net.thumbtack.sharding.test.redis;

import fj.F;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.redis.RedisConnection;
import net.thumbtack.sharding.test.common.Entity;
import net.thumbtack.sharding.test.common.EntityDao;
import redis.clients.jedis.Jedis;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static net.thumbtack.helper.Util.index;

public class RedisDao implements EntityDao {

    private ShardingFacade sharding;

    public RedisDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    @Override
    public Entity select(final long id) {
        return sharding.selectSpec(id, new QueryClosure<Entity>() {
            @Override
            public Entity call(Connection connection) throws Exception {
                Jedis jedis = ((RedisConnection) connection).getClient();
                return selectEntity(jedis, id);
            }
        });
    }

    private Entity selectEntity(Jedis jedis, long id) {
        Entity result = null;
        String keyText = createEntityTextKey(id);
        String keyDate = createEntityDateKey(id);
        String text = jedis.get(keyText);
        String date = jedis.get(keyDate);
        if ((text!=null) && (date!=null)) {
            result = new Entity(id, text, new Date(Long.valueOf(date)));
        }
        return result;
    }

    private String createEntityDateKey(long id) {
        return "Entity:" + id + ":date";
    }

    private String createEntityTextKey(long id) {
        return "Entity:" + id + ":text";
    }

    @Override
    public List<Entity> select(List<Long> ids) {
        return sharding.selectAll(ids, new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                Jedis jedis = ((RedisConnection) connection).getClient();
                List<Long> ids = connection.getCargo();
//                List<String> stringIds = new ArrayList<String>(ids.size());
                List<Entity> result = new ArrayList<Entity>(ids.size());
                for (Long id : ids) {
                    Entity entity = selectEntity(jedis, id);
                    if (entity != null) {
                        result.add(entity);
                    }
                }
                return result;
            }
        });
    }

    @Override
    public List<Entity> selectAll() {
        // Memcached have no method to retrieve all data.
        throw new NotImplementedException();
    }

    @Override
    public Entity insert(final Entity entity) {
        sharding.updateSpec(entity.id, new QueryClosure<Object>() {
            @Override
            public Object call(Connection connection) throws Exception {
                Jedis jedis = ((RedisConnection) connection).getClient();
                return insertEntity(jedis, entity);
            }
        });
        return entity;
    }

    private Object insertEntity(Jedis jedis, Entity entity) {
        String keyText = createEntityTextKey(entity.getId());
        String keyDate = createEntityDateKey(entity.getId());
        jedis.set(keyText, entity.getText());
        jedis.set(keyDate, String.valueOf(entity.getDate().getTime()));
        return null;
    }

    @Override
    public List<Entity> insert(final List<Entity> entities) {
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
                Jedis jedis = ((RedisConnection) connection).getClient();
                int inserted = 0;
                for (Long id: ids) {
                    Entity entity = entityMap.get(id);
                    boolean result = insertEntity(jedis, entity) == null;
                    if (result) {
                        inserted++;
                    }
                }
                return inserted;
            }
        });
        return entities;
    }

    @Override
    public boolean update(final Entity entity) {
        return sharding.updateSpec(entity.id, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                Jedis jedis = ((RedisConnection) connection).getClient();
                return insertEntity(jedis, entity) == null;
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
                Jedis jedis = ((RedisConnection) connection).getClient();
                int inserted = 0;
                for (long id : ids) {
                    Entity entity = entityMap.get(id);
                    boolean result = insertEntity(jedis, entity) == null;
                    if (result) {
                        inserted++;
                    }
                }
                return inserted;
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
                Jedis jedis = ((RedisConnection) connection).getClient();
                return deleteEntity(jedis, id);
            }
        });
    }

    private Boolean deleteEntity(Jedis jedis, long id) {
        String keyText = createEntityTextKey(id);
        String keyDate = createEntityDateKey(id);
        return jedis.del(keyText, keyDate) == 2;
    }

    @Override
    public boolean delete(List<Long> ids) {
        sharding.updateAll(ids, new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
                Jedis jedis = ((RedisConnection) connection).getClient();
                int inserted = 0;
                for (long id : ids) {
                    boolean result = deleteEntity(jedis, id);
                    if (result) {
                        inserted++;
                    }
                }
                return inserted;
            }
        });
        return true;
    }

    @Override
    public boolean deleteAll() {
        // Memcached have no method to retrieve all data.
        throw new NotImplementedException();
    }
}
