package net.thumbtack.sharding.test.redis;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.ShardingBuilder;
import net.thumbtack.sharding.core.query.Query;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.redis.RedisConnection;
import net.thumbtack.sharding.impl.redis.RedisShard;
import net.thumbtack.sharding.test.common.AbstractStorage;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RedisStorage extends AbstractStorage {

    private ShardingFacade sharding;

    public RedisStorage(boolean isSync) throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("redis.properties"));
        List<RedisShard> shards = RedisShard.fromProperties(shardProps);
        ShardingBuilder builder = new ShardingBuilder();
        builder.setShards(shards);
        Map<Long, Query> queryMap = getQueryMap(isSync);
        for (long queryId : queryMap.keySet()) {
            builder.addQuery(queryId, queryMap.get(queryId));
        }
        sharding = new ShardingFacade(builder.build());
    }

    @Override
    public void reset() throws Exception {
        sharding.updateAll(new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.sharding.core.query.Connection connection) throws Exception {
                Jedis jedis = ((RedisConnection) connection).getClient();
//                return memcachedClient.delete(String.valueOf(userId));
                // todo clear redis server.
                return null;
            }
        });
    }

    @Override
    public ShardingFacade sharding() {
        return sharding;
    }
}
