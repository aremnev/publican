package net.thumbtack.shardcon.test.jdbc;

import net.thumbtack.helper.Util;
import net.thumbtack.shardcon.ShardingFacade;
import net.thumbtack.shardcon.Storage;
import net.thumbtack.shardcon.chunk.ChunkEngine;
import net.thumbtack.shardcon.core.Shard;
import net.thumbtack.shardcon.core.ShardingBuilder;
import net.thumbtack.shardcon.core.query.Query;
import net.thumbtack.shardcon.core.query.QueryClosure;
import net.thumbtack.shardcon.impl.jdbc.JdbcConnection;
import net.thumbtack.shardcon.impl.jdbc.JdbcShard;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.thumbtack.shardcon.ShardingFacade.getQueryMap;

public class JdbcStorage implements Storage {

    private ShardingFacade sharding;

    public JdbcStorage(boolean isSync) throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("H2-shard.properties"));
        List<Shard> shards = JdbcShard.fromProperties(shardProps);
        Map<Integer, Shard> bucketToShard = ChunkEngine.mapBucketsFromProperties(shards, shardProps);
        ChunkEngine chunkEngine = new ChunkEngine(bucketToShard);
        ShardingBuilder builder = new ShardingBuilder();
        builder.setShards(shards);
        builder.setKeyMapper(chunkEngine);
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
            public Object call(net.thumbtack.shardcon.core.query.Connection connection) throws Exception {
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
