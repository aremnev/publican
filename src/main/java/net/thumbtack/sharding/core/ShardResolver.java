package net.thumbtack.sharding.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardResolver {

    private Map<Long, Shard> shards = new HashMap<Long, Shard>(0);
    private KeyMapper keyMapper;

    public ShardResolver(Iterable<Shard> shards, KeyMapper keyMapper) {
        for (Shard shard : shards){
            this.shards.put(shard.getId(), shard);
        }
        this.keyMapper = keyMapper;
    }

    public Connection resolveId(long id) {
        long shardId = keyMapper.shard(id);
        Shard shard = shards.get(shardId);
        return createConnection(shard);
    }

    public List<Connection> resolveAll() {
        List<Connection> result = new ArrayList<Connection>(shards.size());
        for (Shard shard : shards.values()) {
            result.add(createConnection(shard));
        }
        return result;
    }

    private Connection createConnection(Shard shard) {
        return new LazyConnection(shard);
    }

    private static class LazyConnection implements Connection {

        private Shard shard;
        private Connection connection;

        public LazyConnection(Shard shard) {
            this.shard = shard;
        }

        @Override
        public void open() {
            if (connection != null) {
                // TODO avoid RuntimeException
                throw new RuntimeException("Connection already is opened");
            }
            connection = shard.getConnection();
        }

        @Override
        public void commit() {
            if (connection == null) {
                // TODO avoid RuntimeException
                throw new RuntimeException("Connection is not opened");
            }
            connection.commit();
        }

        @Override
        public void rollback() {
            if (connection == null) {
                // TODO avoid RuntimeException
                throw new RuntimeException("Connection is not opened");
            }
            connection.rollback();
        }

        @Override
        public void close() {
            if (connection == null) {
                // TODO avoid RuntimeException
                throw new RuntimeException("Connection is not opened");
            }
            connection.close();
        }
    }
}
