package net.thumbtack.sharding.test.jdbc;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.ShardingBuilder;
import net.thumbtack.sharding.core.query.Query;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.impl.jdbc.JdbcShard;
import net.thumbtack.sharding.vbucket.VbucketEngine;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.thumbtack.sharding.ShardingFacade.getQueryMap;

public class JdbcStorage implements Storage {

    private ShardingFacade sharding;

    public JdbcStorage(boolean isSync) throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("H2-shard.properties"));
        List<Shard> shards = JdbcShard.fromProperties(shardProps);
        Map<Integer, Shard> bucketToShard = VbucketEngine.mapBucketsFromProperties(shards, shardProps);
        VbucketEngine vbucketEngine = new VbucketEngine(bucketToShard);
        ShardingBuilder builder = new ShardingBuilder();
        builder.setShards(shards);
        builder.setKeyMapper(vbucketEngine);
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
