package net.thumbtack.sharding.example.jdbc.friends;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.core.ModuloKeyMapper;
import net.thumbtack.sharding.core.QueryConfig;
import net.thumbtack.sharding.core.Sharding;
import net.thumbtack.sharding.core.ShardingConfig;
import net.thumbtack.sharding.core.query.QueryClosure;
import net.thumbtack.sharding.test.jdbc.JdbcConnection;
import net.thumbtack.sharding.test.jdbc.JdbcShardConfig;
import net.thumbtack.sharding.test.jdbc.JdbcShardFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;

public class FriendsStorage implements Storage {

    private ShardingFacade sharding;

    public FriendsStorage() throws Exception {
        ShardingConfig config = new ShardingConfig();
        Properties shardProps = new Properties();
        shardProps.load(Util.getResourceAsReader("H2-shard.properties"));
        List<JdbcShardConfig> shardConfigs = JdbcShardConfig.fromProperties(shardProps);
        config.setShardConfigs(shardConfigs);
        config.setShardFactory(new JdbcShardFactory());
        config.setKeyMapper(new ModuloKeyMapper(shardConfigs.size()));
        Properties queryProps = new Properties();
        queryProps.load(Util.getResourceAsReader("query-async.properties"));
        List<QueryConfig> queryConfigs = QueryConfig.fromProperties(queryProps);
        config.setQueryConfigs(queryConfigs);
        config.setWorkThreads(2);
        sharding = new ShardingFacade(new Sharding(config));
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
