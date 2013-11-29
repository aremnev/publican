package net.thumbtack.shardcon.chunk;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.shardcon.core.KeyMapper;
import net.thumbtack.shardcon.core.QueryLock;
import net.thumbtack.shardcon.core.cluster.*;
import net.thumbtack.shardcon.core.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static net.thumbtack.helper.Util.*;

/**
 *
 */
public class ChunkEngine implements EventProcessor, KeyMapper {

    private static final Logger logger = LoggerFactory.getLogger(ChunkEngine.class);

    private EventListener listener;
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

    public void setQueryLock(QueryLock queryLock) {
        this.queryLock = queryLock;
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
                        if (listener != null) {
                            listener.onEvent(new MovedEvent(bucketId, toShardId));
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
    public void onEvent(Event event) {
        if (event.getId() == ShardAddedEvent.ID) {
            Shard newShard = ((ShardAddedEvent) event).getEventObject();
            shards.put(newShard.getId(), newShard);
        } else if (event.getId() == MovedEvent.ID) {
            MigrationInfo migrationInfo = ((MovedEvent) event).getEventObject();
            mapper.moveBucket(migrationInfo.getBucketId(), migrationInfo.getToShardId());
        }
    }

    @Override
    public void setEventListener(EventListener listener) {
        this.listener = listener;
    }

    @Override
    public int shard(long key) {
        return mapper.shard(key);
    }
}
