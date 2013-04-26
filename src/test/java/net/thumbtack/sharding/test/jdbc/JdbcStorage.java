package net.thumbtack.sharding.test.jdbc;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.core.*;
import net.thumbtack.sharding.core.query.*;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.impl.jdbc.JdbcShard;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.thumbtack.sharding.ShardingFacade.*;

public class JdbcStorage implements Storage {

    private ShardingFacade sharding;

    public JdbcStorage(boolean isSync) throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("H2-shard.properties"));
        List<JdbcShard> shards = JdbcShard.fromProperties(shardProps);
        ShardingBuilder builder = new ShardingBuilder();
        for (JdbcShard shard : shards) {
            builder.addShard(shard);
        }
        Map<Long, Query> queryMap = getQueryMap(isSync);
        for (long queryId : queryMap.keySet()) {
            builder.addQuery(queryId, queryMap.get(queryId));
        }
        sharding = new ShardingFacade(builder.build());
    }

    private Map<Long, Query> getQueryMap(boolean isSync) {
        return isSync ?
                new HashMap<Long, Query>() {
                    {
                        put(SELECT_SPEC_SHARD, new SelectSpecShard());
                        put(SELECT_SHARD, new SelectShard());
                        put(SELECT_ANY_SHARD, new SelectAnyShard());
                        put(SELECT_ALL_SHARDS, new SelectAllShards());
                        put(SELECT_ALL_SHARDS_SUM, new SelectAllShardsSum());
                        put(UPDATE_SPEC_SHARD, new UpdateSpecShard());
                        put(UPDATE_ALL_SHARDS, new UpdateAllShards());
                    }
                } :
                new HashMap<Long, Query>() {
                    {
                        put(SELECT_SPEC_SHARD, new SelectSpecShard());
                        put(SELECT_SHARD, new SelectShardAsync());
                        put(SELECT_ANY_SHARD, new SelectAnyShard());
                        put(SELECT_ALL_SHARDS, new SelectAllShardsAsync());
                        put(SELECT_ALL_SHARDS_SUM, new SelectAllShardsSumAsync());
                        put(UPDATE_SPEC_SHARD, new UpdateSpecShard());
                        put(UPDATE_ALL_SHARDS, new UpdateAllShardsAsync());
                    }
                };
    }

    @Override
    public void reset() throws Exception {
        sharding.updateAll(new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.sharding.core.query.Connection connection) throws Exception {
                Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = Util.readResource("initialize.sql");
                sqlConn.createStatement().execute(sql);
                return null;
            }
        });
    }

    @Override
    public ShardingFacade sharding() {
        return sharding;
    }
}
