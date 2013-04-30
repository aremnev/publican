package net.thumbtack.sharding.test.memcached;

import fj.F;
import net.rubyeye.xmemcached.MemcachedClient;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.memcached.MemcachedConnection;
import net.thumbtack.sharding.test.common.Entity;
import net.thumbtack.sharding.test.common.EntityDao;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static net.thumbtack.helper.Util.index;

public class MemcachedDao implements EntityDao {

    private ShardingFacade sharding;

    public MemcachedDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    @Override
    public Entity select(final long id) {
        return sharding.selectSpec(id, new QueryClosure<Entity>() {
            @Override
            public Entity call(Connection connection) throws Exception {
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                return memcachedClient.get(String.valueOf(id));
            }
        });
    }

    @Override
    public List<Entity> select(List<Long> ids) {
        return sharding.selectAll(ids, new QueryClosure<List<Entity>>() {
            @Override
            public List<Entity> call(Connection connection) throws Exception {
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                List<Long> ids = connection.getCargo();
                List<String> stringIds = new ArrayList<String>(ids.size());
                for (Long myInt : ids) {
                    stringIds.add(String.valueOf(myInt));
                }
                Map<String, Entity> map = memcachedClient.get(stringIds);
                Collection<Entity> col =  map.values();
                List<Entity> result = new ArrayList<Entity>(col.size());
                result.addAll(col);
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
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                return memcachedClient.set(String.valueOf(entity.getId()), 3600, entity);
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
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                int inserted = 0;
                for (long id : ids) {
                    boolean result = memcachedClient.set(String.valueOf(id), 3600, entityMap.get(id));
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
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                return memcachedClient.set(String.valueOf(entity.id), 3600, entity);
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
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                int inserted = 0;
                for (long id : ids) {
                    boolean result = memcachedClient.set(String.valueOf(id), 3600, entityMap.get(id));
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
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                return memcachedClient.delete(String.valueOf(id));
            }
        });
    }

    @Override
    public boolean delete(List<Long> ids) {
        sharding.updateAll(ids, new QueryClosure<Integer>() {
            @Override
            public Integer call(Connection connection) throws Exception {
                List<Long> ids = connection.getCargo();
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                int inserted = 0;
                for (long id : ids) {
                    boolean result = memcachedClient.delete(String.valueOf(id));
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
