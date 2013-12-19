package net.thumbtack.shardcon.chunk.jdbc;

import net.thumbtack.shardcon.chunk.ConfigurationBroker;
import net.thumbtack.shardcon.core.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcConfigurationBroker implements ConfigurationBroker {

    private static final Logger logger = LoggerFactory.getLogger(JdbcConfigurationBroker.class);

    private static final String CREATE_VERSION_TABLE_QUERY = "createVersionTable";
    private static final String SELECT_VERSION_QUERY = "selectVersion";
    private static final String INSERT_VERSION_QUERY = "insertVersion";
    private static final String INC_VERSION_QUERY = "incVersion";
    private static final String CREATE_SHARD_TABLE_QUERY = "createShardTable";
    private static final String SELECT_SHARDS_QUERY = "selectShards";
    private static final String INSERT_SHARD_QUERY = "insertShard";
    private static final String DELETE_SHARD_QUERY = "deleteShard";
    private static final String CREATE_SHARD2CHUNK_MAP_TABLE_QUERY = "createShard2ChunkMapTable";
    private static final String SELECT_SHARD2CHUNK_MAP_QUERY = "selectShard2ChunkMap";
    private static final String INSERT_SHARD2CHUNK_QUERY = "insertShard2Chunk";
    private static final String DELETE_SHARD2CHUNK_QUERY = "deleteShard2Chunk";

    private DataSource dataSource;
    private Properties queriesConfig;

    public JdbcConfigurationBroker(DataSource dataSource, Properties queriesConfig) {
        this.dataSource = dataSource;
        this.queriesConfig = queriesConfig;
    }

    @Override
    public long getVersion() {
        logger.trace("getVersion");
        try(Connection connection = dataSource.getConnection()) {
            Long version = getVersion(connection);
            if (version == null) {
                logger.debug("Version not found, inserting the default version");
                try (Connection conn2update = dataSource.getConnection()) {
                    conn2update.setAutoCommit(false);
                    String createTableSql = queriesConfig.getProperty(CREATE_VERSION_TABLE_QUERY);
                    String insertSql = queriesConfig.getProperty(INSERT_VERSION_QUERY);
                    try {
                        conn2update.prepareStatement(createTableSql).executeUpdate();
                        conn2update.prepareStatement(insertSql).executeUpdate();
                        conn2update.commit();
                    } catch (Exception e) {
                        conn2update.rollback();
                        throw e;
                    }
                }
            }
            return getVersion(connection);
        } catch (Exception e) {
            return handleException("Failed to get version", e);
        }
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
    public long addShard(long version, Shard shard) {
        return 0;
    }

    @Override
    public long removeShard(long version, int shardId) {
        return 0;
    }

    @Override
    public List<Shard> getShards() {
        return null;
    }

    @Override
    public Map<Integer, Integer> getInactiveChunks() {
        return null;
    }

    @Override
    public void removeInactiveChunk(int chunk, int shardFrom) {

    }

    private Long getVersion(Connection connection) {
        try {
            String sql = queriesConfig.getProperty(SELECT_VERSION_QUERY);
            ResultSet rs = connection.prepareStatement(sql).executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.error("Failed to get version", e);
        }
        return null;
    }

    private static long handleException(String msg, Exception e) {
        logger.error(msg, e);
        throw new RuntimeException(msg, e);
    }
}
