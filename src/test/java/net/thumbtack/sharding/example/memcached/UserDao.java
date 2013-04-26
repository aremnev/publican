package net.thumbtack.sharding.example.memcached;

import net.rubyeye.xmemcached.MemcachedClient;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.memcached.MemcachedConnection;

public class UserDao {

    private ShardingFacade sharding;

    public UserDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    public void insert(final long userId, final User user) {
        sharding.updateSpec(userId, new QueryClosure<Object>() {
            @Override
            public Object call(Connection connection) throws Exception {
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                memcachedClient.set(String.valueOf(userId), 3600, user);
                return null;
            }
        });
    }

    public User select(final long userId) {
        return sharding.selectSpec(userId, new QueryClosure<User>() {
            @Override
            public User call(Connection connection) throws Exception {
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                // boolean success=client.touch("key",10);
                return memcachedClient.get(String.valueOf(userId));
            }
        });
    }

    public boolean delete(final long userId) {
        return sharding.updateSpec(userId, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
                return memcachedClient.delete(String.valueOf(userId));
            }
        });
    }
}