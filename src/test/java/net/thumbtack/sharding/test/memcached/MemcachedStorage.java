package net.thumbtack.sharding.test.memcached;

import net.rubyeye.xmemcached.MemcachedClient;
import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.core.ShardingBuilder;
import net.thumbtack.sharding.core.query.Query;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.memcached.MemcachedConnection;
import net.thumbtack.sharding.impl.memcached.MemcachedShard;
import net.thumbtack.sharding.test.common.AbstractStorage;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MemcachedStorage extends AbstractStorage {

    private ShardingFacade sharding;

    public MemcachedStorage(boolean isSync) throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("memcached.properties"));
        List<MemcachedShard> shards = MemcachedShard.fromProperties(shardProps);
        ShardingBuilder builder = new ShardingBuilder();
        for (MemcachedShard shard : shards) {
            builder.addShard(shard);
        }
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
                MemcachedClient memcachedClient  = ((MemcachedConnection) connection).getClient();
//                return memcachedClient.delete(String.valueOf(userId));
                // todo clear memcashed server.
                return null;
            }
        });
    }

    @Override
    public ShardingFacade sharding() {
        return sharding;
    }
}
