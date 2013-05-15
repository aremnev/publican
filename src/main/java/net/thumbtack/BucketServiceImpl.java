package net.thumbtack;

import net.thumbtack.sharding.core.query.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * NB BucketServiceImpl can be running on many machines in same time, so it should be able to synchronize they work correctly.
 */
public class BucketServiceImpl implements BucketService {
    private static final Logger logger = LoggerFactory.getLogger(BucketServiceImpl.class);
    private static final int MAX_LOCK_COUNT = 10;
    private static final int THRESHOLD = 10;
    private static final int TIMEOUT = 1000;

    @Override
    public Result doAction(Action action, long entityId) throws BucketServiceException {
        Result result = null;
        Set<Bucket> buckets = Collections.emptySet();
        try {
            buckets = lockBuckets(action, entityId); // to avoid changing bucket to non-actual state (switching active bucket to inactive etc.).
            result = doAction(buckets, action);
        } finally {
            unlockBuckets(buckets);
        }
        return result;
    }

    @Override
    public Result doReadAction(Action action) throws BucketServiceException {
        // search we can perform without id (by attributes), and without write lock.
        if (action.getActionType() != ActionType.READ) {
            throw new BucketServiceException("method should be used for READ actions only.");
        }

        // TODO implement me.
        return null;
    }

    @Override
    public void createNewReplicaBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException {
        // assumptions:
        //   dstBucket has D state and unused.
        //   srcBucket may A or R.
        // TODO only one worker should be able to move concrete bucket over the all clients (even if on different machines).
        // caller of this method should guarantee it.

        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        Bucket srcBucket = new Bucket(srcShardId, bucketIndex);
        try {
            // blockActivationBucket(bucketIndex)
            // check srcBucket,
            //   if R it should be in findReplicaBuckets(bucketIndex) and bucket shouldn't became desynchronized with active bucket.
            //   if A - continue
            //   else - error
            fullSync(srcBucket, dstBucket);
            setBucketState(dstBucket, BucketState.R);
            addReplicaBucket(dstBucket);
            // now we ready to change srcBucket state to A.
            // unblockActivationBucket(bucketIndex)
            // activateBucketWhenNobodyBlockActivationBucket(srcBucket);
            setBucketState(srcBucket, BucketState.A); // TODO if createNewReplicaBucket or moveActiveBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.
        } catch (BucketServiceException e) {
            // unblockActivationBucket(bucketIndex)
            setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        }
    }

    @Override
    public void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException {
        // assumptions:
        //   dstBucket has D state and unused.
        //   srcBucket may A.
        // TODO only one worker should be able to move concrete bucket over the all clients (even if on different machines).
        // caller of this method should guarantee it.

        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        Bucket srcBucket = new Bucket(srcShardId, bucketIndex);
        try {
            // blockActivationBucket(bucketIndex)
            fullSync(srcBucket, dstBucket);
            // now we ready to change dstBucket state to A.
            // unblockActivationBucket(bucketIndex)
            // activateBucketWhenNobodyBlockActivationBucket(dstBucket)
            setBucketState(dstBucket, BucketState.A); // TODO if createNewReplicaBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.

        } catch (BucketServiceException e) {
            // unblockActivationBucket(bucketIndex)
            setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        }
    }

    @Override
    public void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException {
        // TODO if exist at least one sync process, we shouldn't switch.
        // TODO while we are switching, both Buckets shouldn't change them state.
        // TODO after lock Bucket states, check them.
        Bucket activeBucket = findActiveBucket(bucketIndex);
        setBucketState(activeBucket, BucketState.R);
        waitUntilSomeoneIsUsingBucket(activeBucket);
        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        removeReplicaBucket(dstBucket);
        addReplicaBucket(activeBucket);
        setBucketState(dstBucket, BucketState.A);
    }

    protected Result doAction(Set<Bucket> buckets, Action action) throws BucketServiceException {
        // TODO add aggregation result over all buckets.
        boolean activeBucketIsOk = true;
        ResultBuilderFactory resultBuilderFactory = ResultBuilderFactoryImpl.getInstance();
        ResultBuilder resultBuilder = resultBuilderFactory.createResultBuilderForAction(action);
        for (Bucket bucket : buckets) {
            try {
                Result result = doAction(bucket, action);
                resultBuilder.addResult(result);
            } catch (BucketServiceException e) {
                BucketState bucketState = retrieveBucketState(bucket);
                switch (bucketState) {
                    case A:
                        activeBucketIsOk = false;
                        break;
                    case R:
                        removeReplicaBucket(bucket);
                        break;
                }
            }
        }
        if (!activeBucketIsOk) {
//            switchActiveBucket(); // TODO
        }
        return resultBuilder.build();
    }

    @Override
    public void addShard(String shardId) throws BucketServiceException {
        // TODO move part of A buckets from existing shards to new one.
        // TODO add R buckets to new shard.
    }

    @Override
    public void removeShard(String shardId) throws BucketServiceException {
        // TODO move part of A buckets from existing shards to new one.
        // TODO add R buckets to new shard.
    }

    private void fullSync(Bucket srcBucket, Bucket dstBucket) throws BucketServiceException {
        prepareDstBucket(dstBucket);
        ActionsQueue actionsQueue = createActionsQueue(srcBucket);
        Action action;
        boolean isAlreadySet = false;
        while ((action = pop(actionsQueue, srcBucket)) != null) { // TODO Is it possible, if we block on pop() forever and never will call setSrcBucketState(D)???
            doAction(dstBucket, action);
            if ((!isAlreadySet) && (actionsQueue.count() < THRESHOLD)) { // TODO if other sync process are running on same srcBucket, and has actionsQueue.count() > THRESHOLD, we shouldn't do that.
                setBucketState(srcBucket, BucketState.D);
                isAlreadySet = true;
            }
        }
    }

    private void addReplicaBucket(Bucket newReplicaBucket) {
        // TODO implement me.
    }

    private void removeReplicaBucket(Bucket dstBucket) {
        // TODO implement me.
    }


    protected Action pop(ActionsQueue actionsQueue, Bucket bucket) throws BucketServiceException {
        Action result;
        try {
            while (true) {
                result = actionsQueue.pop();
                if (result == null) {
                    if ((retrieveBucketState(bucket) == BucketState.D) && (retrieveBucketUsageCount(bucket) == 0)) {
                        break;
                    } else {
                        try {
                            wait(TIMEOUT);
                        } catch (InterruptedException e) {
                            logger.error("", e);
                        }
                    }
                }
             }
        } catch (ActionsQueueException e) {
            throw new BucketServiceException(e);
        }
        return result;
    }

    private void waitUntilSomeoneIsUsingBucket(Bucket bucket) throws BucketServiceException {
        while (retrieveBucketUsageCount(bucket) > 0) {
            try {
                wait(TIMEOUT);
            } catch (InterruptedException e) {
                logger.error("", e);
                throw new BucketServiceException(e);
            }
        }
    }

    protected BucketState retrieveBucketState(Bucket bucket) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    protected int retrieveBucketUsageCount(Bucket bucket) {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    protected Bucket findActiveBucket(int bucketIndex) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    protected void lockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO increaseBucketUsageCount()
    }

    protected void unlockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO decreaseBucketUsageCount()
    }

    protected void unlockBuckets(Set<Bucket> buckets) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO decreaseBucketUsageCount()
    }

    protected Bucket lockActiveBucket(int bucketIndex) {
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
        return bucket;
    }

    protected void setBucketState(Bucket bucket, BucketState bucketState) {
        // TODO
    }

    protected void prepareDstBucket(Bucket dstBucket) {
        clearBucket(dstBucket); // need only for fullSync, not for incrementalSync.
        setBucketState(dstBucket, BucketState.P);
    }

    protected Result doAction(Bucket bucket, Action action) throws BucketServiceException {
        Result result = null;
        if (action.getActionType() != ActionType.READ) {
            ActionStorage.getInstance().addAction(bucket, action);
        }
        Connection connection = null;
        connection = open(bucket.getShardId());
        try {
            result = action.call(connection);
        } catch (Exception e) {
            throw new BucketServiceException(e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
//        onActionSuccess(bucket, action);
        return result;
    }

    private Connection open(String shardId) {
        return null;  //TODO implement me.
    }

    protected Set<Bucket> lockBuckets(Action action, long entityId) {
        Set<Bucket> result = new HashSet<Bucket>();
        result.add(lockActiveBucketByEntityId(entityId));
        // TODO
        // if READ, get one Bucket (A or any R).
        // else get all A and R Buckets.
        return result;
    }

    protected Bucket lockActiveBucketByEntityId(Long entityId) {
        return lockActiveBucket(mapEntityIdToBucketIndex(entityId));
    }

    protected int mapEntityIdToBucketIndex(Long entityId) { // TODO impl.
        return 0;
    }

//    protected void onActionSuccess(Bucket bucket, Action action) {
//
//    }

    protected ActionsQueueCombo createActionsQueue(Bucket bucket) {
        return new ActionsQueueCombo(bucket);
    }

    protected void clearBucket(Bucket dstBucket) {
        // todo shard type depended implementation.
    }
}
