package net.thumbtack.shardcon.chunk.jdbc;

import net.thumbtack.shardcon.chunk.ConfigurationBroker;
import org.apache.commons.configuration.AbstractConfiguration;

import javax.sql.DataSource;
import java.util.Map;

public class JdbcConfigurationBroker implements ConfigurationBroker {

    private DataSource dataSource;

    public JdbcConfigurationBroker(DataSource dataSource, AbstractConfiguration queriesConfig) {
        this.dataSource = dataSource;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public Map<Integer, Integer> getChunkToShardMap() {
        return null;
    }

    @Override
    public long moveChunk(long version, int chunk, int shardTo) {
        return 0;
    }

    @Override
    public long addShard(long version, int shardId) {
        return 0;
    }

    @Override
    public Map<Integer, Integer> getInactiveChunks() {
        return null;
    }

    @Override
    public void removeInactiveChunk(int chunk, int shardFrom) {

    }
}
