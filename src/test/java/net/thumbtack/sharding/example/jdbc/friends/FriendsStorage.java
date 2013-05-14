package net.thumbtack.sharding.example.jdbc.friends;

import fj.F;
import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.core.*;
import net.thumbtack.sharding.core.query.*;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.impl.jdbc.JdbcShard;
import net.thumbtack.sharding.vbucket.VbucketEngine;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static net.thumbtack.sharding.ShardingFacade.*;
import static net.thumbtack.helper.Util.*;

public class FriendsStorage implements Storage {

    private ShardingFacade sharding;

    public FriendsStorage() throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("H2-shard.properties"));
        List<Shard> shards = JdbcShard.fromProperties(shardProps);
        Map<Integer, Shard> bucketToShard = mapBucketsFromProperties(shards, shardProps);
        VbucketEngine vbucketEngine = new VbucketEngine(bucketToShard);
        ShardingBuilder builder = new ShardingBuilder();
        builder.setShards(shards);
        builder.setKeyMapper(vbucketEngine.getMapper());
        builder.addQuery(SELECT_SPEC_SHARD, new SelectSpecShard()).
                addQuery(UPDATE_SPEC_SHARD, new UpdateSpecShard()).
                addQuery(UPDATE_ALL_SHARDS, new UpdateAllShardsAsync());
        sharding = new ShardingFacade(builder.build());
    }

    @Override
    public void reset() throws Exception {
        sharding.updateAll(new QueryClosure<Object>() {
            @Override
            public Object call(net.thumbtack.sharding.core.query.Connection connection) throws Exception {
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

    private static Map<Integer, Shard> mapBucketsFromProperties(List<Shard> shards, Properties props) {
        Map<Integer, Shard> shardMap = index(shards, new F<Shard, Integer>() {
            @Override
            public Integer f(Shard shard) {
                return shard.getId();
            }
        });
        Map<Integer, Shard> bucketToShard = new HashMap<Integer, Shard>();
        for (int shardId : shardMap.keySet()) {
            Shard shard = shardMap.get(shardId);
            String[] range = props.getProperty("shard."+ shardId +".vbucketRange").split("-");
            int rangeA = Integer.parseInt(range[0]);
            int rangeB = Integer.parseInt(range[1]);
            for (int i = rangeA; i <= rangeB; i++) {
                bucketToShard.put(i, shard);
            }
        }
        return bucketToShard;
    }
}
