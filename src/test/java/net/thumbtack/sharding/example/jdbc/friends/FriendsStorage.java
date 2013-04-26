package net.thumbtack.sharding.example.jdbc.friends;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.core.*;
import net.thumbtack.sharding.core.query.*;
import net.thumbtack.sharding.impl.jdbc.JdbcConnection;
import net.thumbtack.sharding.impl.jdbc.JdbcShard;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;

import static net.thumbtack.sharding.ShardingFacade.*;

public class FriendsStorage implements Storage {

    private ShardingFacade sharding;

    public FriendsStorage() throws Exception {
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("H2-shard.properties"));
        List<JdbcShard> shards = JdbcShard.fromProperties(shardProps);
        ShardingBuilder builder = new ShardingBuilder();
        for (JdbcShard shard : shards) {
            builder.addShard(shard);
        }
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
}
