package net.thumbtack.shardcon.chunk;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.shardcon.cluster.*;
import net.thumbtack.shardcon.core.*;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static net.thumbtack.helper.Util.*;

/**
 *
 */
public class ChunkEngine implements KeyMapper, ConfigurationApi {

    private static final Logger logger = LoggerFactory.getLogger(ChunkEngine.class);

    private static final String CONFIGURATION_LOCK_NAME = "configuration_lock";

    private final ConfigurationBroker configurationBroker;
    private final MutableLong configurationVersion = new MutableLong();
    private final ChunkMapper mapper;
    private final Map<Integer, Shard> shardMap;
    private final Map<Integer, Chunk> chunkMap;

    private final ExecutorService executor;

    private ShardingCluster shardingCluster;

    private QueryLock queryLock;

    private Lock configurationLock;

    public ChunkEngine(ConfigurationBroker configurationBroker, Iterable<Shard> shards) {
        this.configurationBroker = configurationBroker;
        configurationVersion.setValue(configurationBroker.getVersion());
        Map<Integer, Integer> chunkToShardId = configurationBroker.getChunkToShardMap();
        final long chunkSize = Chunk.chunkSize(chunkToShardId.size());
        mapper = new ChunkMapper(chunkToShardId, new KeyMapper() {
            @Override
            public int shard(long key) {
                return (int) (key / chunkSize);
            }
        });
        shardMap = new ConcurrentHashMap<>(index(shards, new F<Shard, Integer>() {
            @Override
            public Integer f(Shard shard) {
                return shard.getId();
            }
        }));
        chunkMap = index(Chunk.buildChunks(chunkToShardId.size()), new F<Chunk, Integer>() {
            @Override
            public Integer f(Chunk bucket) {
                return bucket.id;
            }
        });
        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("chunk-migration"));
        logger.info("ChunkEngine instantiated");
    }

    public void setShardingCluster(ShardingCluster shardingCluster, List<Long> queriesToLock) {
        configurationLock = shardingCluster.getLock(CONFIGURATION_LOCK_NAME);

        queryLock = new QueryLock(
                shardingCluster.getLock(ShardingBuilder.QUERY_LOCK_NAME),
                shardingCluster.getMutableValue(ShardingBuilder.IS_LOCKED_VALUE_NAME, false),
                queriesToLock
        );
        this.shardingCluster = shardingCluster;
        shardingCluster.addMessageListener(new MessageListener() {
            private MessageDeliveredListener messageDeliveredListener;

            @Override
            public void onMessage(Serializable message) {
                if (message instanceof NewShardEvent) {
                    Shard newShard = ((NewShardEvent) message).getShard();
                    shardMap.put(newShard.getId(), newShard);
                    configurationVersion.setValue(((NewShardEvent) message).getNewVersion());
                } else if (message instanceof MigrationEvent) {
                    MigrationEvent migrationEvent = (MigrationEvent) message;
                    mapper.moveBucket(migrationEvent.getChunkId(), migrationEvent.getToShardId());
                    configurationVersion.setValue(((MigrationEvent) message).getNewVersion());
                }
                messageDeliveredListener.onDelivered(message);
            }

            @Override
            public void setMessageDeliveredListener(MessageDeliveredListener listener) {
                messageDeliveredListener = listener;
            }
        });
        logger.info("ChunkEngine connected to the cluster {}", shardingCluster);
    }

    public static Map<Integer, Shard> mapBucketsFromProperties(List<Shard> shards, Properties props) {
        Map<Integer, Shard> shardMap = index(shards, new F<Shard, Integer>() {
            @Override
            public Integer f(Shard shard) {
                return shard.getId();
            }
        });
        Map<Integer, Shard> bucketToShard = new HashMap<>();
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

    @Override
    public int shard(long key) {
        return mapper.shard(key);
    }

    @Override
    public Map<Integer, Integer> getChunkToShardMap() {
        return configurationBroker.getChunkToShardMap();
    }

    @Override
    public void migrateChunk(final int chunk, final int shardTo, MigrationHelper migrationHelper) {
        Chunk bucket = chunkMap.get(chunk);
        Shard fromShard = shardMap.get(mapper.shard(bucket.fromId));
        Shard toShard = shardMap.get(shardTo);
        logger.info("Starting migration of bucket {} from shard {} to shard {}", chunk, fromShard.getId(), shardTo);
        final Migration migration = new Migration(bucket, fromShard, toShard, migrationHelper);
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                lockConfiguration();
                lockQueries();
                try {
                    if (migration.call()) {
                        if (shardingCluster != null) {
                            configurationVersion.setValue(configurationBroker.moveChunk(configurationVersion.longValue(), chunk, shardTo));
                            shardingCluster.sendMessage(new MigrationEvent(chunk, shardTo, configurationVersion.longValue()));
                        }
                    }
                } finally {
                    unlockQueries();
                    unlockConfiguration();
                }
                return null;
            }
        });
    }

    @Override
    public void addShard(Shard shard) {
        logger.info("Adding new shard {}", shard);
        lockConfiguration();
        try {
            configurationVersion.setValue(configurationBroker.addShard(configurationVersion.longValue(), shard));
            NewShardEvent event = new NewShardEvent(shard, configurationVersion.longValue());
            shardingCluster.sendMessage(event);
            try {
                shardingCluster.waitDelivery(NewShardEvent.class);
            } catch (InterruptedException e) {
                logger.warn("waitDelivery of {} interrupted", event);
            }
        } finally {
            unlockConfiguration();
        }
    }

    @Override
    public List<Shard> getShards() {
        return configurationBroker.getShards();
    }

    @Override
    public Map<Integer, Integer> getInactiveChunks() {
        return configurationBroker.getInactiveChunks();
    }

    @Override
    public void removeInactiveChunk(final int chunk, final int shardFrom, RemoveHelper removeHelper) {
        final Remove remove = new Remove(chunkMap.get(chunk), shardMap.get(shardFrom), removeHelper);
        executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                lockConfiguration();
                try {
                    if (remove.call()) {
                        configurationBroker.removeInactiveChunk(chunk, shardFrom);
                    }
                } finally {
                    unlockConfiguration();
                }
                return null;
            }
        });
    }

    private void lockConfiguration() {
        if (configurationLock != null) {
            configurationLock.lock();
        }
    }

    private void unlockConfiguration() {
        if (configurationLock != null) {
            configurationLock.unlock();
        }
    }

    private void lockQueries() {
        if (queryLock != null) {
            queryLock.lock();
        }
    }

    private void unlockQueries() {
        if (queryLock != null) {
            queryLock.unlock();
        }
    }
}
