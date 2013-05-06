package net.thumbtack.sharding.example.redis;

import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.query.Connection;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.redis.RedisConnection;
import redis.clients.jedis.Jedis;

public class UserDao {

    private ShardingFacade sharding;

    public UserDao(ShardingFacade sharding) {
        this.sharding = sharding;
    }

    public void insert(final long userId, final User user) {
        sharding.updateSpec(userId, new QueryClosure<Object>() {
            @Override
            public Object call(Connection connection) throws Exception {
                Jedis jedis = ((RedisConnection) connection).getClient();
                return insertUser(jedis, user, userId);
            }
        });
    }

    private Object insertUser(Jedis jedis, User user, long userId) {
        String keyName = "Entity:" + userId + ":name";
        String keyAge = "Entity:" + userId + ":age";
        jedis.set(keyName, user.getName());
        jedis.set(keyAge, String.valueOf(user.getAge()));
        return null;
    }

    public User select(final long userId) {
        return sharding.selectSpec(userId, new QueryClosure<User>() {
            @Override
            public User call(Connection connection) throws Exception {
                User result = null;
                Jedis jedis = ((RedisConnection) connection).getClient();
                String keyName = "Entity:" + userId + ":name";
                String keyAge = "Entity:" + userId + ":age";
                String name = jedis.get(keyName);
                String strAge = jedis.get(keyAge);
                if ((name != null) && (strAge != null)) {
                    result = new User(name, Integer.valueOf(strAge));
                }
                return result;
            }
        });
    }

    public boolean delete(final long userId) {
        return sharding.updateSpec(userId, new QueryClosure<Boolean>() {
            @Override
            public Boolean call(Connection connection) throws Exception {
                Jedis jedis = ((RedisConnection) connection).getClient();
                return deleteUser(jedis, userId);
            }
        });
    }

    private Boolean deleteUser(Jedis jedis, long id) {
        String keyName = "Entity:" + id + ":name";
        String keyAge = "Entity:" + id + ":age";
        jedis.del(keyName, keyAge);
        return true;
    }
}