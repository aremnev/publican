package net.thumbtack.sharding.example.memcached;

import net.rubyeye.xmemcached.MemcachedClient;
import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.core.ShardingBuilder;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.core.query.SelectSpecShard;
import net.thumbtack.sharding.core.query.UpdateAllShardsAsync;
import net.thumbtack.sharding.core.query.UpdateSpecShard;
import net.thumbtack.sharding.impl.memcached.MemcachedConnection;
import net.thumbtack.sharding.impl.memcached.MemcachedShard;

import java.util.List;
import java.util.Properties;

import static net.thumbtack.sharding.ShardingFacade.*;

public class UserStorage implements Storage {

    private ShardingFacade sharding;

    public UserStorage() throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("memcached.properties"));
        List<MemcachedShard> shards = MemcachedShard.fromProperties(shardProps);
        ShardingBuilder builder = new ShardingBuilder();
        for (MemcachedShard shard : shards) {
            builder.addShard(shard);
        }
        builder.addQuery(SELECT_SPEC_SHARD, new SelectSpecShard()).
                addQuery(UPDATE_SPEC_SHARD, new UpdateSpecShard()).
                addQuery(UPDATE_ALL_SHARDS, new UpdateAllShardsAsync());
        sharding = new ShardingFacade(builder.build());
    }

    @Override
    public void reset() throws Exception {
        sharding.updateAll(new QueryClosure<Void>() {
            @Override
            public Void call(net.thumbtack.sharding.core.query.Connection connection) throws Exception {
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
