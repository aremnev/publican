package net.thumbtack.shardcon.chunk;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.shardcon.cluster.*;
import net.thumbtack.shardcon.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

import static net.thumbtack.helper.Util.*;

/**
 *
 */
public class ChunkEngine implements KeyMapper {

    private static final Logger logger = LoggerFactory.getLogger(ChunkEngine.class);

    private MessageSender messageSender;
    private ChunkMapper mapper;
    private Map<Integer, Shard> shards;
    private Map<Integer, Chunk> buckets;
    private ExecutorService executor;
    private QueryLock queryLock;

    public ChunkEngine(Map<Integer, Shard> bucketToShard) {
        Map<Integer, Integer> bucketToShardId = new HashMap<>(bucketToShard.size());
        for (int bucketId : bucketToShard.keySet()) {
            bucketToShardId.put(bucketId, bucketToShard.get(bucketId).getId());
        }
        final long bucketSize = Chunk.bucketSize(bucketToShard.size());
        mapper = new ChunkMapper(bucketToShardId, new KeyMapper() {
            @Override
            public int shard(long key) {
                return (int) (key / bucketSize);
            }
        });
        shards = new ConcurrentHashMap<>(index(bucketToShard.values(), new F<Shard, Integer>() {
            @Override
            public Integer f(Shard shard) {
                return shard.getId();
            }
        }));
        buckets = index(Chunk.buildBuckets(bucketToShard.size()), new F<Chunk, Integer>() {
            @Override
            public Integer f(Chunk bucket) {
                return bucket.id;
            }
        });
        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("chunk-migration"));
    }

    public void setShardingCluster(ShardingCluster shardingCluster, List<Long> queriesToLock) {
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
                    shards.put(newShard.getId(), newShard);
                } else if (message instanceof MigrationEvent) {
                    MigrationEvent migrationEvent = (MigrationEvent) message;
                    mapper.moveBucket(migrationEvent.getChunkId(), migrationEvent.getToShardId());
                }
            }
        });
    }

    public void migrate(final int bucketId, final int toShardId, MigrationHelper helper) {
        Chunk bucket = buckets.get(bucketId);
        Shard fromShard = shards.get(mapper.shard(bucket.fromId));
        Shard toShard = shards.get(toShardId);
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
                            messageSender.sendMessage(new MigrationEvent(bucketId, toShardId));
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
}
