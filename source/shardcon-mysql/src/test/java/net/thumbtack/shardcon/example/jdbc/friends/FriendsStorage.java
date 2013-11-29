package net.thumbtack.shardcon.example.jdbc.friends;

import net.thumbtack.helper.Util;
import net.thumbtack.shardcon.ShardingFacade;
import net.thumbtack.shardcon.Storage;
import net.thumbtack.shardcon.core.*;
import net.thumbtack.shardcon.core.query.*;
import net.thumbtack.shardcon.impl.jdbc.JdbcConnection;
import net.thumbtack.shardcon.impl.jdbc.JdbcShard;
import net.thumbtack.shardcon.vbucket.VbucketEngine;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.thumbtack.shardcon.ShardingFacade.*;

public class FriendsStorage implements Storage {

    private ShardingFacade sharding;

    public FriendsStorage() throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("H2-shard.properties"));
        List<Shard> shards = JdbcShard.fromProperties(shardProps);
        Map<Integer, Shard> bucketToShard = VbucketEngine.mapBucketsFromProperties(shards, shardProps);
        VbucketEngine vbucketEngine = new VbucketEngine(bucketToShard);
        ShardingBuilder builder = new ShardingBuilder();
        builder.setShards(shards);
        builder.setKeyMapper(vbucketEngine);
        builder.addQuery(SELECT_SPEC_SHARD, new SelectSpecShard()).
                addQuery(UPDATE_SPEC_SHARD, new UpdateSpecShard()).
                addQuery(UPDATE_ALL_SHARDS, new UpdateAllShardsAsync());
        sharding = new ShardingFacade(builder.build());
    }

    @Override
    public void reset() throws Exception {
        sharding.updateAll(new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.shardcon.core.query.Connection connection) throws Exception {
                Connection sqlConn = ((JdbcConnection) connection).getConnection();
                String sql = Util.readResource("friends-initialize.sql");
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
