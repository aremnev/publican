package net.thumbtack.sharding.vbucket;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.KeyMapper;
import net.thumbtack.sharding.core.cluster.*;
import net.thumbtack.sharding.core.Shard;
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
public class VbucketEngine implements EventProcessor, KeyMapper {

    private static final Logger logger = LoggerFactory.getLogger(VbucketEngine.class);

    private EventListener listener;
    private VbucketMapper mapper;
    private Map<Integer, Shard> shards;
    private Map<Integer, Vbucket> buckets;
    private ExecutorService executor;
    private ExecutorService waitThread;
    private QueryLock queryLock;

    public VbucketEngine(Map<Integer, Shard> bucketToShard) {
        Map<Integer, Integer> bucketToShardId = new HashMap<Integer, Integer>(bucketToShard.size());
        for (int bucketId : bucketToShard.keySet()) {
            bucketToShardId.put(bucketId, bucketToShard.get(bucketId).getId());
        }
        final long bucketSize = Vbucket.bucketSize(bucketToShard.size());
        mapper = new VbucketMapper(bucketToShardId, new KeyMapper() {
            @Override
            public int shard(long key) {
                return (int) (key / bucketSize);
            }
        });
        shards = new ConcurrentHashMap<Integer, Shard>(index(bucketToShard.values(), new F<Shard, Integer>() {
            @Override
            public Integer f(Shard shard) {
                return shard.getId();
            }
        }));
        buckets = index(Vbucket.buildBuckets(bucketToShard.size()), new F<Vbucket, Integer>() {
            @Override
            public Integer f(Vbucket bucket) {
                return bucket.id;
            }
        });
        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("chunk-migration"));
        waitThread = Executors.newSingleThreadExecutor(new NamedThreadFactory("wait-migration"));
    }

    public void setShardingCluster(ShardingCluster cluster) {
        queryLock = cluster.getQueryLock();
        cluster.addEventProcessor(this);
    }

    public void migrate(final int bucketId, final int toShardId, VbucketMigrationHelper helper) {
        Vbucket bucket = buckets.get(bucketId);
        Shard fromShard = shards.get(mapper.shard(bucket.fromId));
        Shard toShard = shards.get(toShardId);
        logger.info("Starting migration of bucket {} from shard {} to shard {}", bucketId, fromShard.getId(), toShardId);
        final VbucketMigration migration = new VbucketMigration(bucket, fromShard, toShard, helper);
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (queryLock != null) {
                    queryLock.lock();
                }
                try {
                    if (migration.call()) {
                        if (listener != null) {
                            listener.onEvent(new VbucketMovedEvent(bucketId, toShardId));
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
        Map<Integer, Shard> bucketToShard = new HashMap<Integer, Shard>();
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
        } else if (event.getId() == VbucketMovedEvent.ID) {
            VbucketMigrationInfo migrationInfo = ((VbucketMovedEvent) event).getEventObject();
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
