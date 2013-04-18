package net.thumbtack.sharding.jdbc;

import net.thumbtack.helper.Util;
import net.thumbtack.sharding.ShardingFacade;
import net.thumbtack.sharding.common.StorageServer;
import net.thumbtack.sharding.core.*;
import net.thumbtack.sharding.core.query.QueryClosure;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcStorageServer implements StorageServer {

    private ShardingFacade sharding;
    private H2Server h2Server;

    public JdbcStorageServer() {
        int shardsCount = 2;
        final List<Shard> shards = new ArrayList<Shard>(shardsCount);
        for (int i = 0; i < shardsCount; i++) {
            String url = "jdbc:h2:mem:test"+ i + ";AUTOCOMMIT=ON;DB_CLOSE_DELAY=-1;MODE=MYSQL;MVCC=TRUE;LOCK_TIMEOUT=120000";
            shards.add(new JdbcShard(i, "org.h2.Driver", url, "sa", ""));
        }
        final KeyMapper keyMapper = new ModuloKeyMapper(shardsCount);
        Configuration configuration = new Configuration() {

            @Override
            public QueryRegistry getQueryRegistry() {
                Properties props = new Properties();
                try {
                    props.load(Util.getResourceAsReader("query.properties"));
                } catch (IOException ignored) {}
                return new QueryRegistry(props);
            }

            @Override
            public Iterable<Shard> getShards() {
                return shards;
            }

            @Override
            public KeyMapper getKeyMapper() {
                return keyMapper;
            }
        };
        sharding = new ShardingFacade(new Sharding(configuration));
        h2Server = new H2Server();
    }

    @Override
    public void start() throws Exception {
        h2Server.start();
    }

    @Override
    public void stop() throws Exception {
        h2Server.stop();
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
