package net.thumbtack.shardcon.chunk;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.shardcon.cluster.*;
import net.thumbtack.shardcon.core.*;
import org.apache.commons.lang.mutable.MutableLong;
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

    private MessageSender messageSender;

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
    }

    public void setShardingCluster(ShardingCluster shardingCluster, List<Long> queriesToLock) {
        configurationLock = shardingCluster.getLock(CONFIGURATION_LOCK_NAME);

        queryLock = new QueryLock(
                shardingCluster.getLock(ShardingBuilder.QUERY_LOCK_NAME),
                shardingCluster.getMutableValue(ShardingBuilder.IS_LOCKED_VALUE_NAME, false),
                queriesToLock
        );
        messageSender = shardingCluster;
        shardingCluster.addMessageListener(new MessageListener() {
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
            }
        });
    }

    public void migrate(final int bucketId, final int toShardId, MigrationHelper helper) {
        Chunk bucket = chunkMap.get(bucketId);
        Shard fromShard = shardMap.get(mapper.shard(bucket.fromId));
        Shard toShard = shardMap.get(toShardId);
        logger.info("Starting migration of bucket {} from shard {} to shard {}", bucketId, fromShard.getId(), toShardId);
        final Migration migration = new Migration(bucket, fromShard, toShard, helper);
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (queryLock != null) {
                    queryLock.lock();
                }
                try {
                    if (migration.call()) {
                        if (messageSender != null) {
//                            messageSender.sendMessage(new MigrationEvent(bucketId, toShardId));
                        }
                        migration.finish();
                    }
                } finally {
                    if (queryLock != null) {
                        queryLock.unlock();
                    }
                }
                return null;
            }
        });
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
    public void moveChunk(int chunk, int shardTo) {

    }

    @Override
    public void addShard(Shard shard) {
        logger.info("Adding new shard {}", shard);
        lockConfiguration();
        try {
            configurationVersion.setValue(configurationBroker.addShard(configurationVersion.longValue(), shard.getId()));
            messageSender.sendMessage(new NewShardEvent(shard, configurationVersion.longValue()));
        } finally {
            unlockConfiguration();
        }
    }

    @Override
    public Map<Integer, Integer> getInactiveChunks() {
        return null;
    }

    @Override
    public void removeInactiveChunk(int chunk, int shardFrom) {

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
}
