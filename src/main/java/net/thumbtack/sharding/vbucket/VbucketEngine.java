package net.thumbtack.sharding.vbucket;

import fj.F;
import net.thumbtack.helper.NamedThreadFactory;
import net.thumbtack.sharding.core.KeyMapper;
import net.thumbtack.sharding.core.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static net.thumbtack.helper.Util.*;

/**
 *
 */
public class VbucketEngine {

    private static final Logger logger = LoggerFactory.getLogger(VbucketEngine.class);

    private OnMigrationFinishListener onMigrationFinishListener;
    private VbucketMapper mapper;
    private Map<Integer, Shard> shards;
    private Map<Integer, Vbucket> buckets;
    private ExecutorService executor;
    private ExecutorService waitThread;

    public VbucketEngine(Map<Integer, Shard> bucketToShard) {
        Map<Integer, Integer> bucketToShardId = new HashMap<Integer, Integer>(bucketToShard.size());
        for (int bucketId : bucketToShard.keySet()) {
            bucketToShardId.put(bucketId, bucketToShard.get(bucketId).getId());
        }
        final long bucketSize = Vbucket.bucketSize(bucketToShard.size());
        mapper = new VbucketMapper(bucketToShardId, new KeyMapper() {
            @Override
            public int shard(long key) {
                return (int) (key / bucketSize + 1);
            }
        });
        shards = index(bucketToShard.values(), new F<Shard, Integer>() {
            @Override
            public Integer f(Shard shard) {
                return shard.getId();
            }
        });
        buckets = index(Vbucket.buildBuckets(bucketToShard.size()), new F<Vbucket, Integer>() {
            @Override
            public Integer f(Vbucket bucket) {
                return bucket.id;
            }
        });
        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("chunk-migration"));
        waitThread = Executors.newSingleThreadExecutor(new NamedThreadFactory("wait-migration"));
    }

    public void migrate(final int bucketId, final int toShardId, VbucketMigrationHelper helper) {
        Vbucket bucket = buckets.get(bucketId);
        Shard fromShard = shards.get(mapper.shard(bucket.fromId));
        Shard toShard = shards.get(toShardId);
        logger.debug("Starting migration of bucket {} from shard {} to shard {}", bucketId, fromShard.getId(), toShardId);
        VbucketMigration migration = new VbucketMigration(bucket, fromShard, toShard, helper);
        final Future<Boolean> future = executor.submit(migration);
        waitThread.execute(new Runnable() {
            @Override
            public void run() {
                boolean success = false;
                try {
                    success = future.get();
                } catch (Exception e) {
                    logger.error("Error during wait of bucket migration", e);
                }
                if (success && onMigrationFinishListener != null) {
                    onMigrationFinishListener.onFinish(bucketId, toShardId);
                }
            }
        });
    }

    public KeyMapper getMapper() {
        return mapper;
    }

    public void setOnMigrationFinishListener(OnMigrationFinishListener onMigrationFinishListener) {
        this.onMigrationFinishListener = onMigrationFinishListener;
    }

    public interface OnMigrationFinishListener {

        void onFinish(int bucketId, int toShard);
    }
}
