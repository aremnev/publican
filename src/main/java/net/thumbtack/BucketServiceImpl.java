package net.thumbtack;

import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

/**
 * NB BucketServiceImpl can be running on many machines in same time, so it should be able to synchronize they work correctly.
 */
public class BucketServiceImpl implements BucketService {
    private static final Logger logger = LoggerFactory.getLogger(BucketServiceImpl.class);
    private static final int MAX_LOCK_COUNT = 10;

    private static final int TIMEOUT = 1000;

    private ShardService shardService;

    @Override
    public Iterator<Integer> getAllBucketIndexIterator() {
        // TODO return set of bucketIndex for all buckets.
        return null;
    }

    @Override
    public Result doAction(String shardId, Action action) throws BucketServiceException {
        Result result = null;
        Connection connection = null;
        Shard shard = shardService.findShard(shardId);
        connection = shard.getConnection();
        try {
            result = action.call(connection);
        } catch (Exception e) {
//            onShardError(shardId);
            throw new BucketServiceException(e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
//        onActionSuccess(bucket, action);
        return result;
    }

    @Override
    public int mapEntityIdToBucketIndex(Long entityId) { // TODO impl.
        return 0;
    }

    @Override
    public void addReplicaBucket(Bucket newReplicaBucket) {
        // TODO implement me.
    }
    @Override
    public void removeReplicaBucket(Bucket dstBucket) {
        // TODO implement me.
    }

    @Override
    public Collection<String> findReplicaShardIds(int bucketIndex) {
        // TODO implement me.
        return null;
    }

    @Override
    public int retrieveBucketUsageCount(Bucket bucket) {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public Bucket findActiveBucket(int bucketIndex) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public Collection<Bucket> findActiveBucketsOnShard(String shardId) {
        // if on bucketIndex exists at least one replica and (active replica absent or exist on shardId), then add bucket(bucketIndex, shardId) to result,
        // returns Collection of Active buckets on shardId even if shard is died.
        // TODO implement me.
        return null;
    }

    @Override
    public void setBucketState(Bucket bucket, BucketState bucketState) {
        // TODO
    }

    @Override
    public BucketState retrieveBucketState(Bucket bucket) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    /**
     *  to avoid changing bucket to non-actual state (switching active bucket to inactive etc.).
     * @param bucketIndex
     * @return
     */
    @Override
    public void lockBucketIndex(int bucketIndex) {
        // TODO
        Bucket bucket = null;
        for (int i = 0; i < MAX_LOCK_COUNT; i++) {
            bucket = findActiveBucket(bucketIndex);
            lockBucket(bucket);
            BucketState bucketState = retrieveBucketState(bucket);
            if (bucketState == BucketState.A) {
                break;
            }
            unlockBucket(bucket);
            //            sleep();//???
        }
//        return bucket;
    }

    protected void lockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO increaseBucketUsageCount()
    }

    protected void unlockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO decreaseBucketUsageCount()
    }

    /**
     *  to avoid changing bucket to non-actual state (switching active bucket to inactive etc.).
     * @param bucketIndex
     * @return
     */
    @Override
    public void unlockBucketIndex(int bucketIndex) {
        // TODO
    }

    @Override
    public long retrieveLastAcceptedAction(Bucket bucket) throws BucketServiceException {
        return 0;  // TODO
    }

    @Override
    public void updateLastAcceptedAction(Bucket bucket, Long actionId) throws BucketServiceException {
        // TODO
    }

//    protected void onActionSuccess(Bucket bucket, Action action) {
//
//    }

    @Override
    public void waitUntilSomeoneIsUsingBucket(Bucket bucket) throws BucketServiceException {
        while (retrieveBucketUsageCount(bucket) > 0) {
            try {
                wait(TIMEOUT);
            } catch (InterruptedException e) {
                logger.error("", e);
                throw new BucketServiceException(e);
            }
        }
    }
}
