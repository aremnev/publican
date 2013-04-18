package net.thumbtack.sharding.jdbc;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.common.Storage;
import net.thumbtack.sharding.core.*;
import net.thumbtack.sharding.core.query.QueryClosure;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcStorage implements Storage {

    private ShardingFacade sharding;

    public JdbcStorage() throws Exception {
        int shardsCount = 2;
        ShardingConfig config = new ShardingConfig();
        List<JdbcShardConfig> shardConfigs = new ArrayList<JdbcShardConfig>();
        for (int i = 0; i < shardsCount; i++) {
            shardConfigs.add(new JdbcShardConfig(
                    i,
                    "org.h2.Driver",
                    "jdbc:h2:mem:test"+ i + ";AUTOCOMMIT=ON;DB_CLOSE_DELAY=-1;MODE=MYSQL;MVCC=TRUE;LOCK_TIMEOUT=120000",
                    "sa",
                    ""
            ));
        }
        config.setShardConfigs(shardConfigs);
        config.setShardFactory(new JdbcShardFactory());
        config.setKeyMapper(new ModuloKeyMapper(shardsCount));
        Properties queryProps = new Properties();
        try {
            queryProps.load(Util.getResourceAsReader("query-async.properties"));
        } catch (IOException ignored) {}
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
